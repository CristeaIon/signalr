package io.github.cristeaion.signalr

import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.hasSetTextAction
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performTextReplacement
import androidx.test.ext.junit.runners.AndroidJUnit4
import io.github.cristeaion.signalr.mockserver.MockServer
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Drives the sample app's real UI on a device against a mock server running inside the test
 * process, so no host-side server or `adb reverse` is needed.
 */
@RunWith(AndroidJUnit4::class)
class ChatFlowTest {

    @get:Rule
    val rule = createAndroidComposeRule<MainActivity>()

    private val server = MockServer()
    private var host = ""

    @Before
    fun startServer() {
        host = "http://127.0.0.1:${server.start(0)}"
    }

    @After
    fun stopServer() {
        server.stop()
    }

    private fun waitForText(text: String, substring: Boolean = false) {
        rule.waitUntil(TIMEOUT_MS) {
            rule.onAllNodes(hasText(text, substring = substring)).fetchSemanticsNodes().isNotEmpty()
        }
    }

    private fun field(label: String) = rule.onNode(hasSetTextAction() and hasText(label))

    private fun click(text: String) = rule.onNodeWithText(text).performScrollTo().performClick()

    private fun login(password: String = "111111") {
        field("Auth Server URL").performTextReplacement("$host/api/login")
        field("Password").performTextReplacement(password)
        click("Login & Extract SID")
    }

    /** The dashboard pre-fills the default port; point it at this test's server. */
    private fun useTestServerInHubUrl() {
        val hubField = rule.onNode(hasSetTextAction() and hasText("/signalr/negotiate", substring = true))
        val current = hubField.fetchSemanticsNode().config[SemanticsProperties.EditableText].text
        hubField.performTextReplacement(current.replace(DemoServer.HOST, host))
    }

    private fun loginAndConnect() {
        login()
        waitForText("Active SID: demo~@~", substring = true)
        useTestServerInHubUrl()
        click("Connect")
        waitForText("CONNECTED")
    }

    private fun sendChat(message: String) {
        field("Message").performScrollTo().performTextReplacement(message)
        rule.onNodeWithContentDescription("Send message").performClick()
    }

    private fun invoke(preset: String) {
        // The method-name field can hold the same text as the preset chip; click the chip.
        rule.onNode(hasText(preset) and hasSetTextAction().not()).performScrollTo().performClick()
        click("Invoke (Wait)")
    }

    private fun countExact(text: String) = rule.onAllNodes(hasText(text)).fetchSemanticsNodes().size

    @Test
    fun wrongPasswordShowsInvalidCredentials() {
        login(password = "wrong1")
        waitForText("Invalid credentials", substring = true)
    }

    @Test
    fun chatMessageAppearsOnceFollowedByBotReply() {
        loginAndConnect()
        sendChat("Hello from the test")

        val botReply = "Hi sa, you said: \"Hello from the test\""
        waitForText(botReply)
        rule.waitForIdle()
        assertEquals(1, countExact("Hello from the test"))
        assertEquals(1, countExact(botReply))
    }

    @Test
    fun twoUsersSeeEachOtherJoinChatAndLeave() = runBlocking {
        loginAndConnect()

        val bobSid = (DemoAuthService(baseUrl = "$host/api/login").login("bob@example.com", "111111") as LoginResult.Success).sid
        val bob = HubConnectionBuilder.create(
            DemoAuthService.updateSessionIdInUrl(DemoServer.SIGNALR_URL.replace(DemoServer.HOST, host), bobSid)
        ).withLogLevel(SignalRLogLevel.WARN).build()
        val bobInbox = Channel<Pair<String, String>>(Channel.UNLIMITED)
        bob.on<String, String>("ReceiveMessage") { user, message -> bobInbox.trySend(user to message) }
        bob.connect()

        waitForText("[Event] UserJoined: \"bob@example.com\"", substring = true)

        bob.send("Send", "Hi from bob")
        waitForText("Hi from bob")

        sendChat("Hey bob")
        withTimeout(TIMEOUT_MS) {
            while (true) {
                if (bobInbox.receive() == ("sa@example.com" to "Hey bob")) break
            }
        }

        invoke("GetOnlineUsers")
        waitForText("Invoke 'GetOnlineUsers' Result: [\"bob@example.com\",\"sa@example.com\"]", substring = true)

        bob.disconnect()
        waitForText("[Event] UserLeft: \"bob@example.com\"", substring = true)
    }

    @Test
    fun historyReturnsEarlierMessages() {
        loginAndConnect()
        sendChat("Remember me")
        waitForText("Hi sa, you said: \"Remember me\"")

        invoke("GetHistory")
        waitForText("Invoke 'GetHistory' Result:", substring = true)
        waitForText("\"message\":\"Remember me\"", substring = true)
    }

    @Test
    fun serverInvokeFailureIsShown() {
        loginAndConnect()
        server.hub.faults.failNextInvoke.set(true)
        invoke("GetHistory")
        waitForText("Invoke error: Invocation '1' failed: Mock failure", substring = true)
    }

    @Test
    fun serverDropDisconnectsAndReconnectWorks() {
        loginAndConnect()
        runBlocking { server.hub.dropAll() }
        waitForText("DISCONNECTED")

        click("Connect")
        waitForText("CONNECTED")
    }

    @Test
    fun rejectedNegotiateIsShownAndNextConnectSucceeds() {
        login()
        waitForText("Active SID: demo~@~", substring = true)
        useTestServerInHubUrl()
        server.hub.faults.rejectNextNegotiate.set(true)

        click("Connect")
        waitForText("Negotiate failed with status 500", substring = true)

        click("Connect")
        waitForText("CONNECTED")
    }

    @Test
    fun placeholderSessionIsRejected() {
        click("Skip / Use Placeholder Session ID")
        useTestServerInHubUrl()
        click("Connect")
        waitForText("Negotiate failed with status 401", substring = true)
        assertEquals(0, countExact("CONNECTED"))
    }

    private companion object {
        const val TIMEOUT_MS = 10_000L
    }
}
