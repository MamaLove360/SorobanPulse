import * as vscode from 'vscode';
import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import { getApiKey } from './apiKeyManager';
import { SseParser } from './eventStream';

// ---------------------------------------------------------------------------
// Live Event Panel (Issue #1316)
//
// A dedicated Activity Bar panel showing real-time events from the Soroban
// Pulse API via EventSource (SSE). Includes pause/resume, clear, and filtering.
// ---------------------------------------------------------------------------

export class SorobanPulseLivePanel {
    public static readonly viewType = 'sorobanpulse.livePanel';
    private static instance: SorobanPulseLivePanel | undefined;

    private readonly panel: vscode.WebviewPanel;
    private readonly context: vscode.ExtensionContext;
    private stream: http.ClientRequest | undefined;
    private disposables: vscode.Disposable[] = [];

    static open(context: vscode.ExtensionContext): void {
        if (SorobanPulseLivePanel.instance) {
            SorobanPulseLivePanel.instance.panel.reveal();
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            SorobanPulseLivePanel.viewType,
            'Live Events',
            vscode.ViewColumn.Two,
            { enableScripts: true }
        );

        SorobanPulseLivePanel.instance = new SorobanPulseLivePanel(panel, context);
    }

    private constructor(panel: vscode.WebviewPanel, context: vscode.ExtensionContext) {
        this.panel = panel;
        this.context = context;

        panel.webview.html = this.getWebviewHtml();

        panel.onDidDispose(() => {
            this.dispose();
            SorobanPulseLivePanel.instance = undefined;
        }, null, this.disposables);

        panel.webview.onDidReceiveMessage(async (message: any) => {
            await this.handleMessage(message);
        }, null, this.disposables);
    }

    private async handleMessage(message: any): Promise<void> {
        switch (message.command) {
            case 'start':
                this.startStream(message.contractId);
                break;
            case 'stop':
                this.stopStream();
                break;
            case 'clear':
                this.panel.webview.postMessage({ command: 'clear' });
                break;
        }
    }

    private startStream(contractId: string): void {
        this.stopStream();

        const base = vscode.workspace.getConfiguration('sorobanpulse').get<string>('baseUrl', 'http://localhost:3000');
        const url = new URL(`${base.replace(/\/$/, '')}/v1/events/stream`);
        url.searchParams.set('contract_id', contractId);

        getApiKey(this.context).then(apiKey => {
            const headers: Record<string, string> = { Accept: 'text/event-stream' };
            if (apiKey) { headers['x-api-key'] = apiKey; }

            const transport = url.protocol === 'https:' ? https : http;
            const req = transport.get(url, { headers }, res => this.onResponse(req, res));
            req.on('error', err => {
                this.panel.webview.postMessage({
                    command: 'error',
                    message: `Connection error: ${err.message}`
                });
            });
            this.stream = req;
        });
    }

    private stopStream(): void {
        if (!this.stream) { return; }
        const req = this.stream;
        this.stream = undefined;
        req.destroy();
    }

    private onResponse(req: http.ClientRequest, res: http.IncomingMessage): void {
        res.setEncoding('utf8');

        if (res.statusCode !== 200) {
            let body = '';
            res.on('data', (chunk: string) => { body += chunk; });
            res.on('end', () => {
                this.panel.webview.postMessage({
                    command: 'error',
                    message: `Server responded ${res.statusCode}: ${body.trim() || res.statusMessage || ''}`
                });
                if (this.stream === req) { this.stream = undefined; }
            });
            return;
        }

        this.panel.webview.postMessage({ command: 'connected' });
        const parser = new SseParser();
        res.on('data', (chunk: string) => {
            for (const message of parser.push(chunk)) {
                try {
                    const event = JSON.parse(message.data);
                    this.panel.webview.postMessage({ command: 'event', event });
                } catch {
                    // Skip unparseable events
                }
            }
        });
        res.on('end', () => {
            if (this.stream === req) {
                this.panel.webview.postMessage({ command: 'closed' });
                this.stream = undefined;
            }
        });
    }

    private getWebviewHtml(): string {
        return `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8" />
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            padding: 12px;
            background: var(--vscode-editor-background);
            color: var(--vscode-editor-foreground);
        }
        .controls {
            display: flex;
            gap: 8px;
            margin-bottom: 12px;
            flex-wrap: wrap;
        }
        input, button {
            padding: 6px 12px;
            border: 1px solid var(--vscode-input-border);
            background: var(--vscode-input-background);
            color: var(--vscode-input-foreground);
            border-radius: 2px;
            font-size: 13px;
            cursor: pointer;
        }
        button:hover { background: var(--vscode-button-hoverBackground); }
        button:active { background: var(--vscode-button-background); }
        input[type="text"] { flex: 1; min-width: 200px; }
        .status {
            padding: 6px 12px;
            border-radius: 2px;
            margin-bottom: 12px;
            font-size: 12px;
        }
        .status.connected { background: rgba(86, 166, 67, 0.2); color: #56a643; }
        .status.error { background: rgba(248, 81, 73, 0.2); color: #f85149; }
        .events {
            border: 1px solid var(--vscode-input-border);
            border-radius: 2px;
            max-height: 500px;
            overflow-y: auto;
            background: var(--vscode-editor-background);
        }
        .event {
            padding: 8px;
            border-bottom: 1px solid var(--vscode-input-border);
            font-family: 'Courier New', monospace;
            font-size: 11px;
            word-break: break-all;
        }
        .event:hover { background: var(--vscode-list-hoverBackground); }
        .event.contract { color: #4ec9b0; }
        .event.system { color: #ce9178; }
        .event.diagnostic { color: #d4d4d4; }
        .empty {
            padding: 12px;
            color: var(--vscode-descriptionForeground);
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="controls">
        <input type="text" id="contractId" placeholder="Contract ID (C…)" />
        <button id="startBtn">Start</button>
        <button id="stopBtn">Stop</button>
        <button id="clearBtn">Clear</button>
    </div>
    <div id="status"></div>
    <div id="events" class="events">
        <div class="empty">No events yet. Enter a contract ID and click Start.</div>
    </div>
    <script>
        const vscode = acquireVsCodeApi();
        const contractId = document.getElementById('contractId');
        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');
        const clearBtn = document.getElementById('clearBtn');
        const status = document.getElementById('status');
        const events = document.getElementById('events');
        let eventCount = 0;

        startBtn.addEventListener('click', () => {
            if (contractId.value.trim()) {
                vscode.postMessage({ command: 'start', contractId: contractId.value.trim() });
            }
        });

        stopBtn.addEventListener('click', () => {
            vscode.postMessage({ command: 'stop' });
        });

        clearBtn.addEventListener('click', () => {
            events.innerHTML = '<div class="empty">Events cleared</div>';
            eventCount = 0;
        });

        window.addEventListener('message', event => {
            const msg = event.data;
            switch (msg.command) {
                case 'connected':
                    status.innerHTML = '<div class="status connected">Connected, listening for events…</div>';
                    if (eventCount === 0) { events.innerHTML = ''; }
                    break;
                case 'closed':
                    status.innerHTML = '<div class="status">Stream closed</div>';
                    break;
                case 'error':
                    status.innerHTML = '<div class="status error">Error: ' + msg.message + '</div>';
                    break;
                case 'event': {
                    const event = msg.event;
                    const type = event.event_type || 'unknown';
                    const div = document.createElement('div');
                    div.className = 'event ' + type.toLowerCase();
                    div.textContent = JSON.stringify(event);
                    events.appendChild(div);
                    eventCount++;
                    events.scrollTop = events.scrollHeight;
                    break;
                }
                case 'clear':
                    events.innerHTML = '<div class="empty">Events cleared</div>';
                    eventCount = 0;
                    break;
            }
        });
    </script>
</body>
</html>`;
    }

    dispose(): void {
        this.stopStream();
        this.disposables.forEach(d => d.dispose());
    }
}
