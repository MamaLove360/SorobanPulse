import { useState } from "react";
import { useWebhookDLQ, useReplayWebhookDLQ } from "../api/hooks.ts";

export default function WebhookDeliveries() {
  const { data: dlqData, isLoading } = useWebhookDLQ();
  const [filterSubscriptionId, setFilterSubscriptionId] = useState("");
  const [filterStartDate, setFilterStartDate] = useState("");
  const replayMutation = useReplayWebhookDLQ();

  const deliveries = dlqData?.data || [];

  const filtered = deliveries.filter((d: any) => {
    if (filterSubscriptionId && d.subscription_id !== filterSubscriptionId) return false;
    if (filterStartDate && new Date(d.created_at) < new Date(filterStartDate)) return false;
    return true;
  });

  const handleReplay = async (id: string) => {
    try {
      await replayMutation.mutateAsync(id);
      alert("Replay queued successfully");
    } catch (error) {
      alert(`Replay failed: ${error}`);
    }
  };

  const handleBulkReplay = async () => {
    if (!confirm(`Replay ${filtered.length} failed deliveries?`)) return;
    try {
      await Promise.all(filtered.map((d: any) => replayMutation.mutateAsync(d.id)));
      alert("All replays queued");
    } catch (error) {
      alert(`Some replays failed: ${error}`);
    }
  };

  const subscriptionIds = Array.from(new Set(deliveries.map((d: any) => d.subscription_id)));

  return (
    <div>
      <div className="page-header">
        <h1>Webhook Delivery Log</h1>
        <p>View failed webhook deliveries and replay dead-letter events.</p>
      </div>

      <div className="card">
        <div className="filters">
          <div className="form-group">
            <label>Filter by Subscription</label>
            <select value={filterSubscriptionId} onChange={(e) => setFilterSubscriptionId(e.target.value)}>
              <option value="">All subscriptions</option>
              {subscriptionIds.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          </div>

          <div className="form-group">
            <label>From Date</label>
            <input
              type="datetime-local"
              value={filterStartDate}
              onChange={(e) => setFilterStartDate(e.target.value)}
            />
          </div>

          {filtered.length > 0 && (
            <button onClick={handleBulkReplay} className="btn btn-secondary" disabled={replayMutation.isPending}>
              Replay All ({filtered.length})
            </button>
          )}
        </div>
      </div>

      <div className="card">
        {isLoading ? (
          <div className="loading">Loading deliveries...</div>
        ) : filtered.length === 0 ? (
          <div className="empty-state">
            <p>✓ No failed deliveries</p>
          </div>
        ) : (
          <table className="deliveries-table">
            <thead>
              <tr>
                <th>Subscription ID</th>
                <th>Status Code</th>
                <th>Response Time</th>
                <th>Error Message</th>
                <th>Timestamp</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((d: any) => (
                <tr key={d.id} className={d.status_code >= 500 ? "error-row" : ""}>
                  <td className="subscription-id">{d.subscription_id?.slice(0, 16)}...</td>
                  <td>
                    <span className={`status-badge status-${d.status_code}`}>{d.status_code}</span>
                  </td>
                  <td>{d.response_time_ms}ms</td>
                  <td className="error-msg">{d.error_message || "-"}</td>
                  <td>{new Date(d.created_at).toLocaleString()}</td>
                  <td>
                    <button
                      onClick={() => handleReplay(d.id)}
                      className="btn btn-sm btn-primary"
                      disabled={replayMutation.isPending}
                    >
                      Replay
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
