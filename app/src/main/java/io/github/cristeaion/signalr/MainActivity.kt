package io.github.cristeaion.signalr

import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material.icons.automirrored.filled.Send
import androidx.compose.material.icons.filled.Clear
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.Refresh
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.input.VisualTransformation
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonArray
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

data class LogEntry(
    val timestamp: String,
    val message: String,
    val type: LogType
)

enum class LogType {
    INFO, SUCCESS, ERROR, EVENT
}

data class ChatEntry(
    val timestamp: String,
    val user: String,
    val message: String
)

fun SignalRLogLevel.toLogType(): LogType = when (this) {
    SignalRLogLevel.ERROR, SignalRLogLevel.WARN -> LogType.ERROR
    SignalRLogLevel.INFO, SignalRLogLevel.VERBOSE, SignalRLogLevel.DEBUG, SignalRLogLevel.NONE -> LogType.INFO
}

/**
 * The payload field holds JSON. A top-level array is the argument list (mirroring classic
 * SignalR's `A`); anything else is passed through as a single argument.
 */
fun parseHubArgs(args: String): Array<Any> {
    val trimmed = args.trim()
    if (trimmed.isBlank()) return emptyArray()
    return try {
        when (val element = SignalRProtocol.json.parseToJsonElement(trimmed)) {
            is JsonArray -> element.toTypedArray()
            else -> arrayOf(element)
        }
    } catch (_: Throwable) {
        arrayOf(trimmed)
    }
}

class MainActivity : ComponentActivity() {

    private var hubConnection: HubConnection? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContent {
            MaterialTheme(
                colorScheme = darkColorScheme(
                    primary = Color(0xFF388E3C),
                    secondary = Color(0xFF0288D1),
                    background = Color(0xFF121212),
                    surface = Color(0xFF1E1E1E),
                    onBackground = Color(0xFFE0E0E0),
                    onSurface = Color(0xFFE0E0E0)
                )
            ) {
                Surface(
                    modifier = Modifier.fillMaxSize(),
                    color = MaterialTheme.colorScheme.background
                ) {
                    var currentScreen by remember { mutableStateOf("LOGIN") }
                    var signalRUrl by remember {
                        mutableStateOf(DemoServer.SIGNALR_URL)
                    }
                    var extractedSid by remember { mutableStateOf("") }

                    if (currentScreen == "LOGIN") {
                        LoginScreen(
                            onLoginSuccess = { sid, _ ->
                                extractedSid = sid
                                signalRUrl = DemoAuthService.updateSessionIdInUrl(signalRUrl, sid)
                                currentScreen = "DASHBOARD"
                            },
                            onSkipToDashboard = {
                                currentScreen = "DASHBOARD"
                            }
                        )
                    } else {
                        SignalRHubApp(
                            initialUrl = signalRUrl,
                            extractedSid = extractedSid,
                            onBackToLogin = {
                                disconnectHub { _, _ -> }
                                currentScreen = "LOGIN"
                            },
                            onConnectRequested = { url, onStateChanged, onLog, onChat ->
                                connectToHub(url, onStateChanged, onLog, onChat)
                            },
                            onDisconnectRequested = { onLog ->
                                disconnectHub(onLog)
                            },
                            onSendRequested = { method, args, onLog ->
                                sendMessage(method, args, onLog)
                            },
                            onInvokeRequested = { method, args, onLog ->
                                invokeMessage(method, args, onLog)
                            }
                        )
                    }
                }
            }
        }
    }

    private suspend fun disconnectHubInternal(onLog: (String, LogType) -> Unit) {
        val oldConn = hubConnection
        hubConnection = null
        if (oldConn != null) {
            try {
                oldConn.disconnect()
                onLog("Disconnected from SignalR hub.", LogType.INFO)
            } catch (t: Throwable) {
                onLog("Disconnect error: ${t.localizedMessage ?: t.message}", LogType.ERROR)
            }
        }
    }

    private fun disconnectHub(onLog: (String, LogType) -> Unit) {
        lifecycleScope.launch {
            disconnectHubInternal(onLog)
        }
    }

    private fun connectToHub(
        url: String,
        onStateChanged: (HubConnectionState) -> Unit,
        onLog: (String, LogType) -> Unit,
        onChat: (String, String) -> Unit
    ) {
        lifecycleScope.launch {
            try {
                disconnectHubInternal(onLog)

                onLog("Creating HubConnection for $url", LogType.INFO)

                // Custom SignalRLogger that forwards internal library logs to the Live Event Log console
                val appLogger = object : SignalRLogger {
                    override fun log(
                        level: SignalRLogLevel,
                        tag: String,
                        message: String,
                        throwable: Throwable?
                    ) {
                        val errSuffix = if (throwable != null) " (Error: ${throwable.localizedMessage ?: throwable.message})" else ""
                        onLog("[$tag] $message$errSuffix", level.toLogType())
                    }
                }

                val connection = HubConnectionBuilder.create(url)
                    .withLogger(appLogger)
                    .withLogLevel(SignalRLogLevel.DEBUG)
                    .withHandshakeTimeout(15_000)
                    .build()
                hubConnection = connection

                connection.onError(lifecycleScope) { throwable ->
                    onLog("[Library onError] ${throwable.localizedMessage ?: throwable.message}", LogType.ERROR)
                }

                // Raw handlers log whatever the hub sends; the typed overload below decodes it.
                listOf("UserJoined", "UserLeft").forEach { event ->
                    connection.on(event) { args ->
                        onLog("[Event] $event: ${args.joinToString(", ")}", LogType.EVENT)
                    }
                }
                // Not onFlow: on a classic connection it currently yields each push twice (once as
                // "ReceiveMessage", once as "chathub.ReceiveMessage").
                connection.on<String, String>("ReceiveMessage") { user, message ->
                    onLog("[Event] ReceiveMessage: $user -> $message", LogType.EVENT)
                    onChat(user, message)
                }

                lifecycleScope.launch {
                    connection.connectionState.collectLatest { state ->
                        onStateChanged(state)
                        onLog("Connection State Changed: $state", LogType.INFO)
                    }
                }

                onLog("Connecting to SignalR Hub...", LogType.INFO)
                connection.connect()
                onLog("Connected successfully!", LogType.SUCCESS)

            } catch (t: Throwable) {
                onLog("Connection failed: ${t.localizedMessage ?: t.message}", LogType.ERROR)
                Log.e("MainActivity", "Failed to connect to SignalR hub", t)
            }
        }
    }

    private fun sendMessage(method: String, args: String, onLog: (String, LogType) -> Unit) {
        val conn = hubConnection
        if (conn == null || conn.connectionState.value != HubConnectionState.CONNECTED) {
            onLog("Cannot send: Not connected to SignalR hub", LogType.ERROR)
            return
        }

        lifecycleScope.launch {
            try {
                onLog("Sending method '$method'...", LogType.INFO)
                conn.send(method, *parseHubArgs(args))
                onLog("Sent '$method' successfully.", LogType.SUCCESS)
            } catch (t: Throwable) {
                onLog("Send error: ${t.localizedMessage ?: t.message}", LogType.ERROR)
            }
        }
    }

    private fun invokeMessage(method: String, args: String, onLog: (String, LogType) -> Unit) {
        val conn = hubConnection
        if (conn == null || conn.connectionState.value != HubConnectionState.CONNECTED) {
            onLog("Cannot invoke: Not connected to SignalR hub", LogType.ERROR)
            return
        }

        lifecycleScope.launch {
            try {
                onLog("Invoking method '$method'...", LogType.INFO)
                val result = conn.invoke(method, *parseHubArgs(args))
                onLog("Invoke '$method' Result: $result", LogType.SUCCESS)
            } catch (t: Throwable) {
                onLog("Invoke error: ${t.localizedMessage ?: t.message}", LogType.ERROR)
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        lifecycleScope.launch {
            try {
                hubConnection?.disconnect()
            } catch (t: Throwable) {
                Log.e("MainActivity", "Error while disconnecting", t)
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun LoginScreen(
    onLoginSuccess: (String, String) -> Unit,
    onSkipToDashboard: () -> Unit
) {
    var authUrl by remember { mutableStateOf(DemoServer.AUTH_URL) }
    var email by remember { mutableStateOf("sa@example.com") }
    var password by remember { mutableStateOf("111111") }
    var isPasswordVisible by remember { mutableStateOf(false) }
    var isLoading by remember { mutableStateOf(false) }
    var errorMessage by remember { mutableStateOf<String?>(null) }
    var lastRawResponse by remember { mutableStateOf<String?>(null) }
    val coroutineScope = rememberCoroutineScope()

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("Demo Authentication", fontWeight = FontWeight.Bold) },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.surface,
                    titleContentColor = MaterialTheme.colorScheme.onSurface
                )
            )
        }
    ) { padding ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(padding)
                .verticalScroll(rememberScrollState())
                .padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
                shape = RoundedCornerShape(16.dp),
                modifier = Modifier.fillMaxWidth()
            ) {
                Column(
                    modifier = Modifier.padding(20.dp),
                    verticalArrangement = Arrangement.spacedBy(12.dp)
                ) {
                    Text(
                        text = "Sign in to Obtain Session ID (sid)",
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.Bold
                    )
                    Text(
                        text = "Logs in against the local mock server and puts the returned session ID into the SignalR URL. Start it with ./gradlew :mockserver:run, then adb reverse tcp:8080 tcp:8080.",
                        style = MaterialTheme.typography.bodySmall,
                        color = Color.Gray
                    )

                    Spacer(Modifier.height(4.dp))

                    OutlinedTextField(
                        value = authUrl,
                        onValueChange = { authUrl = it },
                        label = { Text("Auth Server URL") },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri)
                    )

                    OutlinedTextField(
                        value = email,
                        onValueChange = { email = it },
                        label = { Text("Email / Username") },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Email)
                    )

                    OutlinedTextField(
                        value = password,
                        onValueChange = { password = it },
                        label = { Text("Password") },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        visualTransformation = if (isPasswordVisible) VisualTransformation.None else PasswordVisualTransformation(),
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password),
                        trailingIcon = {
                            IconButton(onClick = { isPasswordVisible = !isPasswordVisible }) {
                                Text(if (isPasswordVisible) "HIDE" else "SHOW", fontSize = 11.sp, color = Color.Gray)
                            }
                        }
                    )

                    if (errorMessage != null) {
                        Surface(
                            color = Color(0xFFD32F2F).copy(alpha = 0.15f),
                            shape = RoundedCornerShape(8.dp),
                            modifier = Modifier.fillMaxWidth()
                        ) {
                            Text(
                                text = errorMessage ?: "",
                                color = Color(0xFFEF5350),
                                style = MaterialTheme.typography.bodySmall,
                                modifier = Modifier.padding(10.dp)
                            )
                        }
                    }

                    Button(
                        onClick = {
                            isLoading = true
                            errorMessage = null
                            coroutineScope.launch {
                                when (val result = DemoAuthService(baseUrl = authUrl).login(email, password)) {
                                    is LoginResult.Success -> {
                                        isLoading = false
                                        lastRawResponse = result.rawResponse
                                        onLoginSuccess(result.sid, result.rawResponse)
                                    }
                                    is LoginResult.Error -> {
                                        isLoading = false
                                        errorMessage = result.errorMessage
                                    }
                                }
                            }
                        },
                        enabled = !isLoading && authUrl.isNotBlank() && email.isNotBlank() && password.isNotBlank(),
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(48.dp),
                        colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF2E7D32))
                    ) {
                        if (isLoading) {
                            CircularProgressIndicator(
                                modifier = Modifier.size(20.dp),
                                color = Color.White,
                                strokeWidth = 2.dp
                            )
                            Spacer(Modifier.width(8.dp))
                            Text("Authenticating...")
                        } else {
                            Text("Login & Extract SID")
                        }
                    }

                    OutlinedButton(
                        onClick = onSkipToDashboard,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text("Skip / Use Placeholder Session ID")
                    }
                }
            }

            if (lastRawResponse != null) {
                Card(
                    colors = CardDefaults.cardColors(containerColor = Color(0xFF0F0F0F)),
                    shape = RoundedCornerShape(12.dp),
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Column(modifier = Modifier.padding(12.dp)) {
                        Text("Raw Response API:", style = MaterialTheme.typography.labelMedium, color = Color.Gray)
                        Text(
                            text = lastRawResponse ?: "",
                            style = MaterialTheme.typography.bodySmall,
                            fontFamily = FontFamily.Monospace,
                            fontSize = 10.sp,
                            color = Color(0xFF81C784)
                        )
                    }
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SignalRHubApp(
    initialUrl: String,
    extractedSid: String,
    onBackToLogin: () -> Unit,
    onConnectRequested: (String, (HubConnectionState) -> Unit, (String, LogType) -> Unit, (String, String) -> Unit) -> Unit,
    onDisconnectRequested: ((String, LogType) -> Unit) -> Unit,
    onSendRequested: (String, String, (String, LogType) -> Unit) -> Unit,
    onInvokeRequested: (String, String, (String, LogType) -> Unit) -> Unit
) {
    var url by remember(initialUrl) { mutableStateOf(initialUrl) }
    var connectionState by remember { mutableStateOf(HubConnectionState.DISCONNECTED) }
    var methodName by remember { mutableStateOf("GetHistory") }
    var methodArgs by remember { mutableStateOf("[]") }
    var chatInput by remember { mutableStateOf("") }

    val logs = remember { mutableStateListOf<LogEntry>() }
    val chat = remember { mutableStateListOf<ChatEntry>() }
    val dateFormat = remember { SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault()) }

    fun addLog(msg: String, type: LogType) {
        val time = dateFormat.format(Date())
        logs.add(0, LogEntry(time, msg, type))
    }

    fun addChat(user: String, message: String) {
        chat.add(ChatEntry(dateFormat.format(Date()).substring(0, 8), user, message))
    }

    val listState = rememberLazyListState()

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("SignalR Chat Client", fontWeight = FontWeight.Bold) },
                navigationIcon = {
                    IconButton(onClick = onBackToLogin) {
                        Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Change Login")
                    }
                },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.surface,
                    titleContentColor = MaterialTheme.colorScheme.onSurface
                ),
                actions = {
                    ConnectionBadge(connectionState)
                }
            )
        }
    ) { padding ->
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(padding)
                .verticalScroll(rememberScrollState())
                .padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            if (extractedSid.isNotBlank()) {
                Surface(
                    color = Color(0xFF2E7D32).copy(alpha = 0.15f),
                    shape = RoundedCornerShape(8.dp),
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Text(
                        text = "Active SID: $extractedSid",
                        color = Color(0xFF81C784),
                        fontSize = 11.sp,
                        fontFamily = FontFamily.Monospace,
                        modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp)
                    )
                }
            }

            // Section 1: Server Configuration
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
                shape = RoundedCornerShape(12.dp),
                modifier = Modifier.fillMaxWidth()
            ) {
                Column(
                    modifier = Modifier.padding(12.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text("Hub Endpoint URL (Dynamic sessionId)", style = MaterialTheme.typography.titleSmall, color = Color.Gray)
                    OutlinedTextField(
                        value = url,
                        onValueChange = { url = it },
                        modifier = Modifier.fillMaxWidth(),
                        maxLines = 3,
                        singleLine = false,
                        textStyle = LocalTextStyle.current.copy(fontSize = 11.sp, fontFamily = FontFamily.Monospace)
                    )

                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        Button(
                            onClick = {
                                onConnectRequested(url, { connectionState = it }, ::addLog, ::addChat)
                            },
                            enabled = connectionState == HubConnectionState.DISCONNECTED,
                            modifier = Modifier.weight(1f),
                            colors = ButtonDefaults.buttonColors(containerColor = Color(0xFF2E7D32))
                        ) {
                            Icon(Icons.Default.PlayArrow, contentDescription = null)
                            Spacer(Modifier.width(4.dp))
                            Text("Connect")
                        }

                        Button(
                            onClick = {
                                onDisconnectRequested(::addLog)
                                connectionState = HubConnectionState.DISCONNECTED
                            },
                            enabled = connectionState != HubConnectionState.DISCONNECTED,
                            modifier = Modifier.weight(1f),
                            colors = ButtonDefaults.buttonColors(containerColor = Color(0xFFC62828))
                        ) {
                            Icon(Icons.Default.Refresh, contentDescription = null)
                            Spacer(Modifier.width(4.dp))
                            Text("Disconnect")
                        }
                    }
                }
            }

            // Section 2: Chat room
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
                shape = RoundedCornerShape(12.dp),
                modifier = Modifier.fillMaxWidth()
            ) {
                Column(
                    modifier = Modifier.padding(12.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text("Chat Room (chathub)", style = MaterialTheme.typography.titleSmall, color = Color.Gray)

                    LazyColumn(
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(220.dp)
                            .background(Color(0xFF0A0A0A), shape = RoundedCornerShape(8.dp))
                            .padding(8.dp),
                        verticalArrangement = Arrangement.spacedBy(6.dp)
                    ) {
                        if (chat.isEmpty()) {
                            item {
                                Text(
                                    "No messages yet. Connect and say hi - the ChatBot answers.",
                                    color = Color.Gray,
                                    fontSize = 12.sp
                                )
                            }
                        }
                        items(chat) { entry -> ChatItem(entry) }
                    }

                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        OutlinedTextField(
                            value = chatInput,
                            onValueChange = { chatInput = it },
                            placeholder = { Text("Message") },
                            modifier = Modifier.weight(1f),
                            singleLine = true
                        )
                        IconButton(
                            onClick = {
                                onSendRequested("Send", "[${SignalRProtocol.json.encodeToString(String.serializer(), chatInput)}]", ::addLog)
                                chatInput = ""
                            },
                            enabled = connectionState == HubConnectionState.CONNECTED && chatInput.isNotBlank()
                        ) {
                            Icon(Icons.AutoMirrored.Filled.Send, contentDescription = "Send message")
                        }
                    }
                }
            }

            // Section 3: Raw Send / Invoke
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
                shape = RoundedCornerShape(12.dp),
                modifier = Modifier.fillMaxWidth()
            ) {
                Column(
                    modifier = Modifier.padding(12.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text("Send / Invoke Method", style = MaterialTheme.typography.titleSmall, color = Color.Gray)

                    OutlinedTextField(
                        value = methodName,
                        onValueChange = { methodName = it },
                        label = { Text("Method Name") },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true
                    )

                    OutlinedTextField(
                        value = methodArgs,
                        onValueChange = { methodArgs = it },
                        label = { Text("Payload / Arguments (JSON)") },
                        modifier = Modifier.fillMaxWidth(),
                        maxLines = 3
                    )

                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        OutlinedButton(
                            onClick = {
                                onSendRequested(methodName, methodArgs, ::addLog)
                            },
                            enabled = connectionState == HubConnectionState.CONNECTED,
                            modifier = Modifier.weight(1f)
                        ) {
                            Icon(Icons.AutoMirrored.Filled.Send, contentDescription = null, modifier = Modifier.size(16.dp))
                            Spacer(Modifier.width(4.dp))
                            Text("Send")
                        }

                        Button(
                            onClick = {
                                onInvokeRequested(methodName, methodArgs, ::addLog)
                            },
                            enabled = connectionState == HubConnectionState.CONNECTED,
                            modifier = Modifier.weight(1f)
                        ) {
                            Text("Invoke (Wait)")
                        }
                    }

                    Text("Presets:", style = MaterialTheme.typography.labelSmall, color = Color.Gray)

                    Row(
                        horizontalArrangement = Arrangement.spacedBy(6.dp),
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        FilterChip(
                            selected = false,
                            onClick = {
                                methodName = "GetHistory"
                                methodArgs = "[]"
                            },
                            label = { Text("GetHistory", fontSize = 11.sp) }
                        )

                        FilterChip(
                            selected = false,
                            onClick = {
                                methodName = "GetOnlineUsers"
                                methodArgs = "[]"
                            },
                            label = { Text("GetOnlineUsers", fontSize = 11.sp) }
                        )

                        FilterChip(
                            selected = false,
                            onClick = {
                                methodName = "Send"
                                methodArgs = "{\"H\": \"chathub\", \"M\": \"Send\", \"A\": [\"Hello from a raw envelope\"]}"
                            },
                            label = { Text("Raw envelope", fontSize = 11.sp) }
                        )
                    }
                }
            }

            // Section 4: Log Console
            Card(
                colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
                shape = RoundedCornerShape(12.dp),
                modifier = Modifier
                    .fillMaxWidth()
                    .height(420.dp)
            ) {
                Column(
                    modifier = Modifier
                        .fillMaxSize()
                        .padding(12.dp)
                ) {
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.SpaceBetween,
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text("Live Event Log (${logs.size})", style = MaterialTheme.typography.titleSmall, color = Color.Gray)
                        IconButton(onClick = { logs.clear() }) {
                            Icon(Icons.Default.Clear, contentDescription = "Clear logs", tint = Color.Gray)
                        }
                    }

                    HorizontalDivider(color = Color.DarkGray, modifier = Modifier.padding(vertical = 4.dp))

                    LazyColumn(
                        state = listState,
                        modifier = Modifier
                            .fillMaxSize()
                            .background(Color(0xFF0A0A0A), shape = RoundedCornerShape(8.dp))
                            .padding(8.dp),
                        verticalArrangement = Arrangement.spacedBy(4.dp)
                    ) {
                        items(logs) { entry ->
                            Log.d("",entry.toString())
                            LogItem(entry)
                        }
                    }
                }
            }
        }
    }
}

@Composable
fun ConnectionBadge(state: HubConnectionState) {
    val (color, text) = when (state) {
        HubConnectionState.CONNECTED -> Color(0xFF4CAF50) to "CONNECTED"
        HubConnectionState.CONNECTING -> Color(0xFFFF9800) to "CONNECTING"
        HubConnectionState.DISCONNECTED -> Color(0xFFF44336) to "DISCONNECTED"
    }

    Surface(
        color = color.copy(alpha = 0.2f),
        contentColor = color,
        shape = CircleShape,
        modifier = Modifier.padding(end = 12.dp)
    ) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 4.dp)
        ) {
            Box(
                modifier = Modifier
                    .size(8.dp)
                    .background(color, CircleShape)
            )
            Spacer(Modifier.width(6.dp))
            Text(text, fontSize = 11.sp, fontWeight = FontWeight.Bold)
        }
    }
}

@Composable
fun ChatItem(entry: ChatEntry) {
    val isBot = entry.user == "ChatBot"
    Column {
        Text(
            text = "${entry.user}  ${entry.timestamp}",
            color = if (isBot) Color(0xFFFFB74D) else Color(0xFF64B5F6),
            fontSize = 11.sp,
            fontWeight = FontWeight.Bold
        )
        Text(text = entry.message, color = Color(0xFFE0E0E0), fontSize = 13.sp)
    }
}

@Composable
fun LogItem(entry: LogEntry) {
    val textColor = when (entry.type) {
        LogType.INFO -> Color(0xFFB0BEC5)
        LogType.SUCCESS -> Color(0xFF81C784)
        LogType.ERROR -> Color(0xFFE57373)
        LogType.EVENT -> Color(0xFF64B5F6)
    }

    Column {
        Text(
            text = "[${entry.timestamp}] ${entry.message}",
            color = textColor,
            fontFamily = FontFamily.Monospace,
            fontSize = 11.sp,
            lineHeight = 14.sp
        )
    }
}
