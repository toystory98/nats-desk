import Cocoa
import WebKit
import Darwin
import Sparkle

@main
final class AppDelegate: NSObject, NSApplicationDelegate, WKNavigationDelegate, WKUIDelegate, WKDownloadDelegate, NSMenuItemValidation {
    private let updaterController = SPUStandardUpdaterController(startingUpdater: false, updaterDelegate: nil, userDriverDelegate: nil)
    private var window: NSWindow!
    private var webView: WKWebView!
    private var backend: Process?
    private var startupTimer: Timer?
    private var startupAttempts = 0
    private var serverURL: URL!
    private var quitting = false
    private var logHandle: FileHandle?

    static func main() {
        let app = NSApplication.shared
        let delegate = AppDelegate()
        app.delegate = delegate
        app.setActivationPolicy(.regular)
        app.run()
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        installMenus()
        updaterController.startUpdater()
        let config = WKWebViewConfiguration()
        config.websiteDataStore = .default()
        webView = WKWebView(frame: .zero, configuration: config)
        webView.navigationDelegate = self
        webView.uiDelegate = self
        webView.allowsBackForwardNavigationGestures = false
        window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1320, height: 860),
                          styleMask: [.titled, .closable, .miniaturizable, .resizable], backing: .buffered, defer: false)
        window.title = "NATS Desk"
        window.minSize = NSSize(width: 700, height: 600)
        window.contentView = webView
        window.center()
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        webView.loadHTMLString("<html><body style='background:#181722;color:#c5b1f0;font:18px -apple-system;display:grid;place-items:center;height:90vh'><p>Starting NATS Desk…</p></body></html>", baseURL: nil)
        do { try startBackend() } catch { showFailure(error.localizedDescription) }
    }

    private func installMenus() {
        let menu = NSMenu()
        let appItem = NSMenuItem(); menu.addItem(appItem)
        let appMenu = NSMenu(); appItem.submenu = appMenu
        appMenu.addItem(withTitle: "About NATS Desk", action: #selector(NSApplication.orderFrontStandardAboutPanel(_:)), keyEquivalent: "")
        let updates = appMenu.addItem(withTitle: "Check for Updates…", action: #selector(SPUStandardUpdaterController.checkForUpdates(_:)), keyEquivalent: "")
        updates.target = updaterController
        let automatic = appMenu.addItem(withTitle: "Automatically Check for Updates", action: #selector(toggleAutomaticUpdates(_:)), keyEquivalent: "")
        automatic.target = self
        appMenu.addItem(.separator())
        appMenu.addItem(withTitle: "Quit NATS Desk", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        let editItem = NSMenuItem(); menu.addItem(editItem)
        let edit = NSMenu(title: "Edit"); editItem.submenu = edit
        for (name, action, key) in [("Undo", "undo:", "z"), ("Cut", "cut:", "x"), ("Copy", "copy:", "c"), ("Paste", "paste:", "v"), ("Select All", "selectAll:", "a")] {
            edit.addItem(withTitle: name, action: Selector(action), keyEquivalent: key)
        }
        let viewItem = NSMenuItem(); menu.addItem(viewItem)
        let view = NSMenu(title: "View"); viewItem.submenu = view
        let reload = view.addItem(withTitle: "Reload Dashboard", action: #selector(reloadDashboard), keyEquivalent: "r"); reload.target = self
        let logs = view.addItem(withTitle: "Show App Data Folder", action: #selector(showDataFolder), keyEquivalent: ""); logs.target = self
        NSApp.mainMenu = menu
    }

    @objc private func toggleAutomaticUpdates(_ sender: NSMenuItem) {
        updaterController.updater.automaticallyChecksForUpdates.toggle()
    }

    func validateMenuItem(_ menuItem: NSMenuItem) -> Bool {
        if menuItem.action == #selector(toggleAutomaticUpdates(_:)) {
            menuItem.state = updaterController.updater.automaticallyChecksForUpdates ? .on : .off
        }
        return true
    }

    private var dataDirectory: URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0].appendingPathComponent("NATS Desk", isDirectory: true)
    }
    @objc private func showDataFolder() { NSWorkspace.shared.open(dataDirectory) }
    @objc private func reloadDashboard() { if let url = serverURL { webView.load(URLRequest(url: url)) } }

    private func startBackend() throws {
        let fm = FileManager.default
        try fm.createDirectory(at: dataDirectory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        // A stable origin preserves WebKit theme settings across launches.
        let port: UInt16 = 48127
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw appError("Cannot allocate a local socket.") }
        var address = sockaddr_in(); address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET); address.sin_port = port.bigEndian
        address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))
        var reuse: Int32 = 1
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size))
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size)) }
        }
        close(fd)
        guard result == 0 else { throw appError("Port 48127 is already in use. Quit the other NATS Desk instance, then reopen this app.") }
        guard let helper = Bundle.main.url(forAuxiliaryExecutable: "nats-desk-server") else { throw appError("The bundled server is missing. Reinstall NATS Desk.") }
        let logURL = dataDirectory.appendingPathComponent("server.log")
        fm.createFile(atPath: logURL.path, contents: nil)
        logHandle = try FileHandle(forWritingTo: logURL)
        let process = Process()
        process.executableURL = helper
        process.arguments = ["-parent-pid", String(getpid()), "-listen", "127.0.0.1:\(port)", "-profiles", dataDirectory.appendingPathComponent("hosts.json").path]
        process.currentDirectoryURL = dataDirectory
        process.standardOutput = logHandle
        process.standardError = logHandle
        process.terminationHandler = { [weak self] proc in
            DispatchQueue.main.async {
                guard let self = self, !self.quitting else { return }
                self.startupTimer?.invalidate()
                self.showFailure("The local dashboard server stopped (code \(proc.terminationStatus)). See server.log in the app data folder for details.")
            }
        }
        backend = process
        try process.run()
        serverURL = URL(string: "http://127.0.0.1:\(port)/")!
        startupTimer = Timer.scheduledTimer(withTimeInterval: 0.25, repeats: true) { [weak self] _ in self?.checkStartup() }
    }

    private func checkStartup() {
        startupAttempts += 1
        if startupAttempts > 80 { startupTimer?.invalidate(); showFailure("The dashboard did not start within 20 seconds. See server.log in the app data folder."); return }
        guard backend?.isRunning == true else { return }
        var request = URLRequest(url: serverURL); request.timeoutInterval = 1
        URLSession.shared.dataTask(with: request) { [weak self] _, response, _ in
            guard (response as? HTTPURLResponse)?.statusCode == 200 else { return }
            DispatchQueue.main.async {
                guard let self = self, self.startupTimer?.isValid == true, self.backend?.isRunning == true else { return }
                self.startupTimer?.invalidate()
                self.webView.load(URLRequest(url: self.serverURL))
            }
        }.resume()
    }
    private func appError(_ message: String) -> NSError { NSError(domain: "NATSDesk", code: 1, userInfo: [NSLocalizedDescriptionKey: message]) }
    private func showFailure(_ message: String) {
        let alert = NSAlert(); alert.messageText = "NATS Desk couldn’t start"; alert.informativeText = message
        alert.addButton(withTitle: "Quit"); alert.addButton(withTitle: "Show App Data")
        if alert.runModal() == .alertSecondButtonReturn { showDataFolder() }
        NSApp.terminate(nil)
    }
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { true }
    func applicationWillTerminate(_ notification: Notification) {
        quitting = true; startupTimer?.invalidate()
        if let process = backend, process.isRunning { process.interrupt(); process.waitUntilExit() }
        try? logHandle?.close()
    }

    func webView(_ webView: WKWebView, decidePolicyFor navigationAction: WKNavigationAction, decisionHandler: @escaping (WKNavigationActionPolicy) -> Void) {
        guard let url = navigationAction.request.url else { decisionHandler(.cancel); return }
        if url.scheme == "about" { decisionHandler(.allow); return }
        if url.scheme == "http", url.host == "127.0.0.1", url.port == serverURL?.port {
            decisionHandler(navigationAction.shouldPerformDownload ? .download : .allow)
        } else {
            if navigationAction.navigationType == .linkActivated, ["http", "https"].contains(url.scheme ?? "") { NSWorkspace.shared.open(url) }
            decisionHandler(.cancel)
        }
    }
    func webView(_ webView: WKWebView, runOpenPanelWith parameters: WKOpenPanelParameters, initiatedByFrame frame: WKFrameInfo, completionHandler: @escaping ([URL]?) -> Void) {
        let panel = NSOpenPanel(); panel.allowsMultipleSelection = parameters.allowsMultipleSelection
        panel.canChooseDirectories = false; panel.canChooseFiles = true
        panel.beginSheetModal(for: window) { completionHandler($0 == .OK ? panel.urls : nil) }
    }
    func webView(_ webView: WKWebView, navigationAction: WKNavigationAction, didBecome download: WKDownload) { download.delegate = self }
    func download(_ download: WKDownload, decideDestinationUsing response: URLResponse, suggestedFilename: String, completionHandler: @escaping (URL?) -> Void) {
        let panel = NSSavePanel(); panel.nameFieldStringValue = suggestedFilename
        panel.beginSheetModal(for: window) { completionHandler($0 == .OK ? panel.url : nil) }
    }
}
