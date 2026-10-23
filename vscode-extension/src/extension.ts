import * as vscode from 'vscode';
import { ApiExplorerProvider, EndpointItem } from './apiExplorer';
import { RequestTesterPanel } from './requestTester';
import { ApiEndpoint } from './types';
import { setApiKey, setAdminApiKey, clearApiKeys, migrateLegacyApiKeys } from './apiKeyManager';
import { testWebhook } from './webhookTester';
import { ContractHoverProvider, HOVER_LANGUAGES } from './contractHoverProvider';
import { SorobanPulseLivePanel } from './livePanel';

export function activate(context: vscode.ExtensionContext): void {
    void migrateLegacyApiKeys(context);

    const explorer = new ApiExplorerProvider();
    const contractHover = new ContractHoverProvider(context);

    // Tree view
    const treeView = vscode.window.createTreeView('sorobanpulse.apiExplorer', {
        treeDataProvider: explorer,
        showCollapseAll: true,
    });

    // Search/filter box above the tree
    const searchBox = vscode.window.createInputBox();
    searchBox.placeholder = 'Filter endpoints…';
    searchBox.onDidChangeValue(v => explorer.setFilter(v));

    // Commands
    context.subscriptions.push(
        treeView,

        vscode.commands.registerCommand('sorobanpulse.refreshExplorer', () => {
            explorer.setFilter('');
            explorer.refresh();
        }),

        vscode.commands.registerCommand('sorobanpulse.openRequestTester', (endpoint?: ApiEndpoint) => {
            RequestTesterPanel.open(context, endpoint);
        }),

        vscode.commands.registerCommand('sorobanpulse.copyUrl', async (item?: EndpointItem) => {
            if (!item) { return; }
            const base = vscode.workspace.getConfiguration('sorobanpulse').get<string>('baseUrl', 'http://localhost:3000');
            const url = base.replace(/\/$/, '') + item.endpoint.path;
            await vscode.env.clipboard.writeText(url);
            vscode.window.showInformationMessage(`Copied: ${url}`);
        }),

        vscode.commands.registerCommand('sorobanpulse.openSettings', () => {
            vscode.commands.executeCommand('workbench.action.openSettings', 'sorobanpulse');
        }),

        // Issue #963: secure API key management (SecretStorage-backed).
        vscode.commands.registerCommand('sorobanpulse.setApiKey', () => setApiKey(context)),
        vscode.commands.registerCommand('sorobanpulse.setAdminApiKey', () => setAdminApiKey(context)),
        vscode.commands.registerCommand('sorobanpulse.clearApiKeys', () => clearApiKeys(context)),

        // Issue #963: webhook test interface.
        vscode.commands.registerCommand('sorobanpulse.testWebhook', () => testWebhook()),

        // Issue #1316: live event panel.
        vscode.commands.registerCommand('sorobanpulse.openLivePanel', () => SorobanPulseLivePanel.open(context)),

        // Issue #1125: contract ID hover.
        vscode.languages.registerHoverProvider(HOVER_LANGUAGES, contractHover),
        vscode.workspace.onDidChangeConfiguration(e => {
            if (e.affectsConfiguration('sorobanpulse')) { contractHover.clear(); }
        }),
    );
}

export function deactivate(): void {
    // Nothing to clean up — disposables handled via context.subscriptions
}
