"""\
Copyright (c) 2014 Justin Wind

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

============================
MUD Client Protocol, Twisted
============================

Based on this specification: http://www.moo.mud.org/mcp2/mcp2.html

"""
from twisted.protocols import basic
from collections import deque
from random import choice
import logging
import string
import shlex
import re


logger = logging.getLogger(__name__)


class MCPError(Exception):
    pass


def generateKey(length, chars):
    """Return a string of length, composed from chars, for use as a key."""
    return ''.join([choice(chars) for i in range(length)])


def versionGEQ(v1, v2):
    """Return True if the MCP version string v1 ("2.1", exempli gratia) is
    greater-than-or-equal to v2."""
    (v1major, v1minor) = v1.split('.', 1)
    (v2major, v2minor) = v2.split('.', 1)
    if int(v1major) > int(v2major):
        return True
    if int(v1major) == int(v2major) and int(v1minor) >= int(v2minor):
        return True
    return False


def versionMin(v1, v2):
    """Return the minimum of two MCP version strings."""
    (v1major, v1minor) = v1.split('.', 1)
    (v2major, v2minor) = v2.split('.', 1)
    if int(v1major) < int(v2major):
        return v1
    elif int(v2major) < int(v1major):
        return v2
    elif int(v1minor) < int(v2minor):
        return v1
    return v2


def versionBest(iRange, rRange):
    """Return the best version common to the two ranges."""
    (iMin, iMax) = iRange
    (rMin, rMax) = rRange
    if versionGEQ(rMax, iMin) and versionGEQ(iMax, rMin):
        return versionMin(iMax, rMax)
    return None


class MCPPackage(object):
    """\
    Bundle of handlers which make up an MCP package.
    """
    packageName = ''
    versionRange = None

    def __init__(self, mcp):
        self.mcp = mcp
        self.version = None

    def attach(self, supportedVersion):
        """\
        Invoked when the other end has indicated it will support this package,
        this method should install the message handlers for the supportedVersion.
        """
        self.version = supportedVersion
        raise NotImplementedError

    def handle(self, message, data):
        """\
        Handle a packageName message here.
        """
        raise NotImplementedError

    def send(self):
        """\
        Send a packageName message here.
        """
        msg = 'package-message'
        self.mcp.sendMessage(msg, None)
        raise NotImplementedError


class MCPPackageNegotiate(MCPPackage):
    """Handle 'mcp-negotiate' commands."""
    packageName = 'mcp-negotiate'
    versionRange = ("1.0", "2.0")

    def __init__(self, mcp):
        MCPPackage.__init__(self, mcp)
        self.negotiated = False

    def attach(self, supportedVersion):
        """Install support for mcp-negotiate commands."""
        if supportedVersion is None:
            # normal packages return here, but negotiate is an integral MCP
            # package, and needs to be able to bootstrap itself
            supportedVersion = "2.0"
        self.version = supportedVersion
        if versionGEQ(supportedVersion, "2.0"):
            self.mcp.messageHandlers[self.packageName + '-end'] = self.handleEnd
        if versionGEQ(supportedVersion, "1.0"):
            self.mcp.messageHandlers[self.packageName + '-can'] = self.handleCan
        logger.debug(
            "attached package '%s' (%s)",
            self.packageName,
            supportedVersion)
        if not versionGEQ(supportedVersion, "2.0"):
            # version 1.0 does not have an end-of-negotiations, so just pretend
            self.handleEnd(None, {})

    def proffer(self):
        """Send the list of packages."""
        for packageName in self.mcp.packagesCapable:
            package = self.mcp.packagesCapable[packageName]
            self.sendCan(package)
        self.sendEnd()

    def sendEnd(self):
        """Send the command indicating no more packages."""
        msg = self.packageName + '-end'
        if self.version is None and not versionGEQ(self.mcp.version, "2.1"):
            # pre-negotiation, but MCP version doesn't support this message
            return
        if self.version is not None and not versionGEQ(self.version, "2.0"):
            # fully negotiated, but mcp-negotiate version doesn't support this message
            return
        self.mcp.sendMessage(msg, None)

    def handleEnd(self, message, data):
        """Receive the end of packages command."""
        self.mcp.negotiated = True
        logger.debug(
            "negotiations complete")
        self.mcp.connectionNegotiated()

    def sendCan(self, package):
        """Send the command indicating a package is available."""
        msg = self.packageName + '-can'
        if self.version is not None and not versionGEQ(self.version, "1.0"):
            # this should never occur, but hey
            return
        self.mcp.sendMessage(msg, {
            'package': package.packageName,
            'min-version': package.versionRange[0],
            'max-version': package.versionRange[1]})

    def handleCan(self, message, data):
        """Receive an available package notification."""
        for requiredKey in ('package', 'min-version', 'max-version'):
            if requiredKey not in data:
                logger.warning(
                    "ignoring '%s' due to missing key '%s'",
                    message,
                    requiredKey)
                return
        if data['package'] not in self.mcp.packagesCapable:
            logger.debug(
                "unsupported package '%s'",
                data['package'])
            return
        package = self.mcp.packagesCapable[data['package']]

        supportedVersion = versionBest(
            (data['min-version'], data['max-version']),
            package.versionRange)
        if supportedVersion is None:
            logger.debug(
                "no version match for package '%s'",
                data['package'])
            return

        package.attach(supportedVersion)


class MCPPackageCord(MCPPackage):
    """Handle 'mcp-cord' comamnds."""
    packageName = 'mcp-cord'
    versionRange = ("1.0", "1.0")

    CORDFORMAT = "%s%d"

    def __init__(self, mcp):
        MCPPackage.__init__(self, mcp)
        self.cordNext = 0
        self.cords = {}

    def attach(self, supportedVersion):
        """Install support for mcp-cord commands."""
        if supportedVersion is None:
            return
        self.version = supportedVersion
        if versionGEQ(supportedVersion, "1.0"):
            self.mcp.messageHandlers[self.packageName + '-open'] = self.handleOpen
            self.mcp.messageHandlers[self.packageName + '-close'] = self.handleClose
            self.mcp.messageHandlers[self.packageName] = self.handle
        logger.debug(
            "attached package %s (%s)",
            self.packageName,
            supportedVersion)

    def sendOpen(self, cordType, cb):
        """\
        Open a cord, return the cord id.
        Callback cb(mcp, type, id, msg, data) will be invoked whenever a cord
        message on the opened id is received, until that cord id is closed."""
        msg = self.packageName + '-open'
        cordID = self.CORDFORMAT % ("I" if self.mcp.initiator else "R", self.cordNext)
        self.cordNext += 1
        self.mcp.sendMessage(msg, {
            '_id': cordID,
            '_type': cordType})
        self.cords[cordID] = (cb, cordType)
        return cordID

    def handleOpen(self, message, data):
        """"""
        for requiredKey in ['_id', '_type']:
            if requiredKey not in data:
                logger.warning(
                    "'%s' missing required key '%s'",
                    message,
                    requiredKey)
                return
        if data['_id'] in self.cords:
            logger.warning(
                "'%s' of duplicate cord '%s'",
                message,
                data['_id'])
            return
        self.cords[data['_id']] = data['_type']
        logger.debug(
            "opened cord '%s'",
            data['_id'])

    def sendClose(self, cordID):
        """"""
        msg = self.packageName + '-close'
        if cordID not in self.cords:
            logger.warning(
                "tried to close non-existant cord '%s'",
                cordID)
            return
        self.mcp.sendMessage(msg, {
            '_id': cordID})

    def handleClose(self, message, data):
        """"""
        if '_id' not in data:
            logger.warning(
                "'%s' missing required key '%s'",
                message,
                '_id')
            return
        if data['_id'] not in self.cords:
            logger.warning(
                "tried to close non-existant cord '%s'",
                data['_id'])
            return
        del self.cords[data['_id']]

    def send(self, cordID, cordMsg, data=None):
        """"""
        msg = self.packageName
        if data is None:
            data = {}
        if '_id' not in self.cords:
            logger.warning(
                "could not send to non-existant cord '%s'",
                cordID)
            return
        data['_id'] = cordID
        data['_message'] = cordMsg
        self.mcp.sendMessage(msg, data)

    def handle(self, message, data):
        """"""
        for requiredKey in ('_id', '_message'):
            if requiredKey not in data:
                logger.warning(
                    "'%s' missing required key '%s'",
                    message,
                    requiredKey)
                return
        if data['_id'] not in self.cords:
            logger.warning(
                "'%s' for non-existant cord '%s'",
                message,
                data['_id'])
            return
        cordID = data['_id']
        cordMsg = data['_message']
        # FIXME: maybe delete _id and _message from data before hitting the
        # callback, because they aren't part of the message proper?
        (cordCallback, cordType) = self.cords[cordID]
        if callable(cordCallback):
            cordCallback(self.mcp, cordType, cordID, cordMsg, data)


# object is inhereted here so that super() will work,
# because Twisted's BaseProtocol is an old-style class
class MCP(basic.LineOnlyReceiver, object):
    """\
    A line-oriented protocol, supporting out-of-band messages.
    """
    VERSION_MIN = "1.0"
    VERSION_MAX = "2.1"

    MCP_HEADER = '#$#'
    MCP_ESCAPE = '#$"'

    AUTHKEY_CHARACTERS = string.ascii_lowercase + \
                         string.ascii_uppercase + \
                         string.digits + \
                         "-~`!@#$%^&()=+{}[]|';?/><.,"
    AUTHKEY_SET = set(AUTHKEY_CHARACTERS)
    AUTHKEY_LEN = 16

    KEY_CHARACTERS = string.ascii_lowercase + \
                     string.ascii_uppercase + \
                     string.digits + \
                     '-'
    KEY_SET = set(KEY_CHARACTERS)
    KEY_LEN = 6

    def __init__(self, initiator=False):
        """\
        Create a new MCP protocol.

        If initiator is True, proffer handshake; otherwise respond to it.
        This only affects the initial handshake.
        """
        self.initiator = initiator
        # Which side of the conversation we are.

        self.version = None
        # The state of the connection handshake.
        # This will be set to the running protocol version, once established.

        self.authKey = ''
        # This connection's authentication key.
        # Blank until handshake complete.

        self.inProgress = {}
        # A list of multi-line messages which have not yet been terminated,
        # indexed by their _data-tag property.

        self.packagesCapable = {}
        # All the packages we know how to handle.

        self.packagesActive = {}
        # The packages the remote side proffered via negotiation,
        # which matched ones we can deal with.

        self.messageHandlers = {}
        # A dispatch table mapping messages to the package functions which
        # will handle them.

        self.sendQueue = deque()
        # A list of lines to transmit once the handshake has been completed.

        self.sendMessageQueue = deque()
        # A list of messages to transmit once the package negotiations have completed.

        self.negotiated = False
        # True once package negotiations have completed.

        # register the standard packages
        self.addPackage(MCPPackageNegotiate)
        self.addPackage(MCPPackageCord)

        # bootstrap support for the negotiation package
        self.packagesCapable[MCPPackageNegotiate.packageName].attach(None)


    def connectionMade(self):
        """Send the initiator handshake on connection."""

        self._peer = self.transport.getPeer().host

        logger.debug("connectionMade, peer is '%s", self._peer)


        if self.initiator:
            self.sendMessage('mcp', {
                'version': MCP.VERSION_MIN,
                'to': MCP.VERSION_MAX})


    def connectionEstablished(self):
        """Called when the MCP handshake has been completed."""
        # send our package negotiations
        self.packagesCapable[MCPPackageNegotiate.packageName].proffer()
        # and flush our queue of pending normal data
        while True:
            try:
                self.sendLine(self.sendQueue.popleft())
            except IndexError:
                break


    def connectionNegotiated(self):
        """Called when MCP package exchange has completed."""
        logger.debug("connection negotiated, flushing queued messages")
        while True:
            try:
                (message, kvs) = self.sendMessageQueue.popleft()
            except IndexError:
                break
            self.sendMessage(message, kvs)


    def addPackage(self, packageClass, *args, **kwargs):
        """Register a package type as one we are capable of handling."""
        if not issubclass(packageClass, MCPPackage):
            raise MCPError("cannot install unknown package type")

        pkg = packageClass(self, *args, **kwargs)
        self.packagesCapable[pkg.packageName] = pkg


    class __InProgress(object):
        """\
        An unterminated multi-line stanza, waiting for completion.

        data is kept distinct from multiData to ease checking of collisions.
        the keys used to store data are all collapsed to lowercase.
        """
        def __init__(self, message):
            self.message = message
            self.data = {}
            self.multiData = {}

        def setKey(self, key, data):
            if key in self.multiData:
                logger.warning(
                    "ignoring attempt to overwrite multiline key '%s' with single value",
                    key)
                return
            self.data[key] = data

        def setMultiKey(self, key, data):
            if key in self.data:
                logger.warning(
                    "ignoring attempt to overwrite single value key '%s' with multiline datum",
                    key)
                return
            if key not in self.multiData:
                self.multiData[key] = []
            if data is not None:
                self.multiData[key].append(data)

        def allData(self):
            """Return the combined simple and multikey data."""
            return dict(self.multiData, **self.data)

    def __multiKeyEnd(self, datatag):
        if datatag not in self.inProgress:
            logger.warning(
                "termination of unknown multi-line stanza '%s'",
                datatag)
            return

        self._dispatchMessage(
            self.inProgress[datatag].message,
            self.inProgress[datatag].allData())
        del self.inProgress[datatag]

    def __multiKeyContinue(self, line):
        try:
            (datatag, line) = re.split(r'\s+', line, 1)
        except ValueError:
            (datatag, line) = (line, '')

        if datatag not in self.inProgress:
            logger.warning(
                "continuation of unknown multi-line stanza '%s'",
                datatag)
            return
        inProgress = self.inProgress[datatag]

        try:
            (key, line) = line.split(': ', 1)
        except ValueError:
            (key, line) = (line, '')

        key = key.tolower()
        if key in inProgress.data:
            logger.warning(
                "multi-line stanza '%s' tried to update non-multi-line key '%s'",
                datatag,
                key)
            return
        if key not in inProgress.multiData:
            logger.warning(
                "multi-line stanza '%s' tried to update untracked key '%s'",
                datatag,
                key)
            return

        inProgress.data[key].append(line)
        self.messageUpdate(datatag, key)

    def __lineParseMCP(self, line):
        """Process an out-of-band message."""

        line = line[len(MCP.MCP_HEADER):]

        try:
            (message, line) = re.split(r'\s+', line, 1)
        except ValueError:
            (message, line) = (line, '')

        if message == ':': # end of multi-line stanza
            self.__multiKeyEnd(line)

        elif message == '*': # continuation of multi-line stanza
            self.__multiKeyContinue(line)

        else: # simple message
            # "#$#message authkey [k: v [...]]"
            inProgress = MCP.__InProgress(message)
            multiline = False

            if self.version:
                try:
                    (authKey, line) = re.split(r'\s+', line, 1)
                except ValueError:
                    (authKey, line) = (line, '')

                if authKey != self.authKey:
                    logger.warning(
                        "ignoring message with foreign key '%s'",
                        authKey)
                    return

            lexer = shlex.shlex(line, posix=True)
            lexer.commenters = ''
            lexer.quotes = '"'
            lexer.whitespace_split = True
            try:
                for key in lexer:
                    # keys are case-insensitive, normalize here
                    key = key.lower()

                    if key[-1] != ':':
                        logger.warning(
                            "message '%s' could not parse key '%s'",
                            message,
                            key)
                        return
                    key = key[:-1]

                    if key[0] not in string.ascii_lowercase:
                        logger.warning(
                            "message '%s' ignored due to invalid key '%s'",
                            message,
                            key)
                        return
                    if not set(key).issubset(MCP.KEY_SET):
                        logger.warning(
                            "message '%s' ignored due to invalid key '%s'",
                            message,
                            key)
                        return

                    try:
                        value = next(lexer)
                    except StopIteration:
                        logger.warning(
                            "message '%s' has key '%s' without value",
                            message,
                            key)
                        return

                    if key[-1] == '*':
                        key = key[:-1]
                        if key in inProgress.multiData or key in inProgress.data:
                            logger.warning(
                                "message '%s' ignoring duplicate key '%s'",
                                message,
                                key)
                            continue
                        inProgress.multiData[key] = []
                        multiline = True
                    else:
                        if key in inProgress.data or key in inProgress.multiData:
                            logger.warning(
                                "message '%s' ignoring duplicate key '%s'",
                                message,
                                key)
                            continue
                        inProgress.data[key] = value

            except ValueError:
                logger.warning(
                    "message '%s' has unparsable data",
                    message)
                return

            if multiline:
                if '_data-tag' not in inProgress.data:
                    logger.warning(
                        "ignoring message with multi-line variables but no _data-tag")
                    return
                self.inProgress[inProgress.data['_data-tag']] = inProgress
                self.messageUpdate(inProgress.data['_data-tag'], None)
            else:
                self.__dispatchMessage(inProgress.message, inProgress.allData())


    def messageUpdate(self, datatag, key):
        """\
        Called when a multiline message has received a new line, but has not
        completed.

        Generally ignorable, but some servers awkwardly use multiline messages
        as continuous channels.
        Override this to handle such a beast.
        """
        pass


    def __dispatchMessage(self, message, data):
        """Invoke the handler function for a message."""
        logger.debug(
            "MCP message: %s %s",
            message,
            repr(data))

        # handle handshaking messages directly
        if message == 'mcp':
            self.__handshake(message, data)
        else:
            if message in self.messageHandlers:
                self.messageHandlers[message](message, data)
            else:
                self.messageReceived(message, data)


    def __handshake(self, message, data):
        """Handle 'mcp' messages, which establish a connection."""
        if self.version:
            logger.warning(
                "ignoring handshake message during established session")
            return

        if 'version' not in data:
            logger.warning(
                "%s did not send enough version information",
                "responder" if self.initiator else "initiator")
            return
        if 'to' not in data:
            data['to'] = data['version']
        supportedVersion = versionBest(
            (MCP.VERSION_MIN, MCP.VERSION_MAX),
            (data['version'], data['to']))
        if supportedVersion is None:
            logger.warning(
                "handshake failed, incompatible versions")
            # FIXME: maybe raise exception on this
            return

        if self.initiator:
            if 'authentication-key' in data:
                if not set(data['authentication-key']).issubset(MCP.AUTHKEY_SET):
                    logger.warning(
                        "responder proffered unexpected characters in authentication-key")
                    return
                self.authKey = data['authentication-key']
                logger.debug(
                    "client started new session with key '%s'",
                    self.authKey)
            else:
                logger.warning(
                    "ignoring message '%s' before session established",
                    message)
                return
        else:
            authKey = generateKey(MCP.AUTHKEY_LEN, MCP.AUTHKEY_CHARACTERS)
            # send before setting, as handshake message doesn't include authkey
            self.sendMessage('mcp', {
                'authentication-key': authKey,
                'version': MCP.VERSION_MIN,
                'to': MCP.VERSION_MAX})
            self.authKey = authKey
            logger.debug(
                "established new session (%s) with key '%s'",
                supportedVersion,
                authKey)

        self.version = supportedVersion
        self.connectionEstablished()


    def lineReceived(self, line):
        """Process a received line for MCP messages."""
        if line.startswith(MCP.MCP_HEADER):
            self.__lineParseMCP(line)
        else:
            if line.startswith(MCP.MCP_ESCAPE):
                line = line[len(MCP.MCP_ESCAPE):]
            self.lineReceivedInband(line)


    def sendLine(self, line):
        """
        Sends a line of normal data.
        """

        if not self.initiator and self.version is None:
            self.sendQueue.append(line)
            return

        if line.startswith((MCP.MCP_HEADER, MCP.MCP_ESCAPE)):
            line = ''.join([MCP.MCP_ESCAPE, line])
        super(MCP, self).sendLine(line)


    def sendMessage(self, message, kvs=None):
        """
        Sends an MCP message, with data.
        """
        # FIXME: this is janky
        # queue non-core messages until after package negotiation
        if not self.negotiated:
            if not message.startswith('mcp'):
                logger.debug(
                    "deferred MCP-send of '%s'",
                    message)
                self.sendMessageQueue.append((message, kvs))
                return

        datatag = None
        msg = []
        line = [MCP.MCP_HEADER, message]
        if self.authKey is not '':
            line.extend([' ', self.authKey])
        if kvs is not None:
            for k, v in kvs.iteritems():
                if isinstance(v, basestring) and '\n' not in v:
                    line.extend([' ', k, ': "', v, '"'])
                else:
                    if not datatag:
                        datatag = generateKey(MCP.KEY_LEN, MCP.KEY_CHARACTERS)
                    line.extend([' ', k, '*: ""'])
                    if not isinstance(v, basestring):
                        vLines = v
                    else:
                        vLines = v.split('\n')
                    for l in vLines:
                        msg.append(''.join([MCP.MCP_HEADER, '* ', datatag, ' ', k, ': ', l]))
        msg.insert(0, ''.join(line))
        for m in msg:
            super(MCP, self).sendLine(m)
            logger.debug(
                "MCP-send: %s",
                m)


    def lineReceivedInband(self, line):
        """
        Called when there's a line of normal data to process.

        Override in implementation.
        """
        print "in: ", line
        return
        raise NotImplementedError


    def messageReceived(self, message, data):
        """
        Called when there's an otherwise-unhandled MCP message.
        """
        logger.warning(
            "unhandled message '%s' %s",
            message,
            repr(data))
        pass


if __name__ == '__main__':
    from twisted.internet import reactor
    from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
    import sys, os.path

    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) < 3:
        print "Usage: %s <host> <port>" % (os.path.basename(sys.argv[0]))
        sys.exit(64)

    HOST = sys.argv[1]
    PORT = int(sys.argv[2])

    def gotMCP(p):
        print 'got'
#        p.sendLine("WHO")
#        reactor.callLater(1, p.sendLine("QUIT"))
#        reactor.callLater(2, p.transport.loseConnection)

    print "establishing endpoing"
    point = TCP4ClientEndpoint(reactor, HOST, PORT)
    print "connecting"
    d = connectProtocol(point, MCP())
    print "adding things"
    d.addCallback(gotMCP)
    print "running"
    reactor.run()
