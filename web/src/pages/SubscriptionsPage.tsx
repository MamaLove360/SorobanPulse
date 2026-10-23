import { useState } from "react";
import { useSubscriptions, useCreateSubscription, useUpdateSubscription, useDeleteSubscription } from "../api/hooks.ts";

export default function SubscriptionsPage() {
  const { data: subscriptionsData, isLoading } = useSubscriptions();
  const [showForm, setShowForm] = useState(false);
  const [formData, setFormData] = useState({ contractId: "", topic: "", callbackUrl: "" });
  const [errors, setErrors] = useState<Record<string, string>>({});

  const subscriptions = subscriptionsData?.data || [];
  const createMutation = useCreateSubscription();
  const deleteMutation = useDeleteSubscription();

  const validateForm = () => {
    const newErrors: Record<string, string> = {};
    const contractIdRegex = /^C[A-Z0-9]{55}$/;
    if (!contractIdRegex.test(formData.contractId)) newErrors.contractId = "Invalid contract ID format";
    if (!formData.callbackUrl.startsWith("http")) newErrors.callbackUrl = "Must be a valid URL";
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validateForm()) return;

    try {
      await createMutation.mutateAsync({
        callback_url: formData.callbackUrl,
        contract_id: formData.contractId,
        event_type: formData.topic,
      });
      setFormData({ contractId: "", topic: "", callbackUrl: "" });
      setShowForm(false);
    } catch (error) {
      setErrors({ submit: String(error) });
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm("Delete this subscription?")) return;
    try {
      await deleteMutation.mutateAsync(id);
    } catch (error) {
      alert(`Failed to delete: ${error}`);
    }
  };

  return (
    <div>
      <div className="page-header">
        <h1>Subscriptions</h1>
        <p>Manage webhook and alert subscriptions for contract events.</p>
        <button onClick={() => setShowForm(!showForm)} className="btn btn-primary">
          {showForm ? "Cancel" : "New Subscription"}
        </button>
      </div>

      {showForm && (
        <div className="card">
          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label>Contract ID</label>
              <input
                type="text"
                placeholder="C..."
                value={formData.contractId}
                onChange={(e) => setFormData({ ...formData, contractId: e.target.value })}
                className={errors.contractId ? "input-error" : ""}
              />
              {errors.contractId && <span className="error">{errors.contractId}</span>}
            </div>

            <div className="form-group">
              <label>Event Type (optional)</label>
              <input
                type="text"
                placeholder="e.g., contract, system"
                value={formData.topic}
                onChange={(e) => setFormData({ ...formData, topic: e.target.value })}
              />
            </div>

            <div className="form-group">
              <label>Callback URL</label>
              <input
                type="url"
                placeholder="https://..."
                value={formData.callbackUrl}
                onChange={(e) => setFormData({ ...formData, callbackUrl: e.target.value })}
                className={errors.callbackUrl ? "input-error" : ""}
              />
              {errors.callbackUrl && <span className="error">{errors.callbackUrl}</span>}
            </div>

            {errors.submit && <span className="error">{errors.submit}</span>}

            <button type="submit" className="btn btn-primary" disabled={createMutation.isPending}>
              {createMutation.isPending ? "Creating..." : "Create"}
            </button>
          </form>
        </div>
      )}

      <div className="card">
        {isLoading ? (
          <div className="loading">Loading subscriptions...</div>
        ) : subscriptions.length === 0 ? (
          <div className="empty-state">
            <p>No subscriptions yet.</p>
            <button onClick={() => setShowForm(true)} className="btn btn-secondary">
              Create one →
            </button>
          </div>
        ) : (
          <table className="subscriptions-table">
            <thead>
              <tr>
                <th>Contract ID</th>
                <th>Event Type</th>
                <th>Callback URL</th>
                <th>Enabled</th>
                <th>Last Delivered</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {subscriptions.map((sub: any) => (
                <tr key={sub.id}>
                  <td className="contract-id">{sub.contract_id?.slice(0, 16)}...</td>
                  <td>{sub.event_type || "all"}</td>
                  <td className="callback-url">{sub.callback_url}</td>
                  <td>{sub.enabled ? "✓" : "✗"}</td>
                  <td>{sub.last_delivered_at ? new Date(sub.last_delivered_at).toLocaleString() : "Never"}</td>
                  <td>
                    <button onClick={() => handleDelete(sub.id)} className="btn btn-sm btn-danger">
                      Delete
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
