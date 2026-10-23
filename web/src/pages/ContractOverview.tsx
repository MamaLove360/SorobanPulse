import { useParams, Link } from "react-router-dom";
import { AreaChart, Area, BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { useEventsByContract, useContractStatsHistory } from "../api/hooks.ts";

export default function ContractOverview() {
  const { contractId } = useParams<{ contractId: string }>();
  if (!contractId) return <div>Contract ID not found</div>;

  const { data: eventsData, isLoading: eventsLoading } = useEventsByContract(contractId, { limit: 10 });
  const { data: statsData, isLoading: statsLoading } = useContractStatsHistory(contractId, { period: "1h", limit: 24 });

  const events = eventsData?.data || [];
  const stats = statsData?.data || [];

  const eventTypeCounts = events.reduce((acc, e) => {
    const type = e.event_type || "unknown";
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const chartData = Object.entries(eventTypeCounts).map(([name, value]) => ({
    name,
    value,
  }));

  const colors = ["#8b5cf6", "#ec4899", "#3b82f6", "#10b981", "#f59e0b"];

  return (
    <div>
      <div className="page-header">
        <h1>Contract Overview</h1>
        <p className="contract-id">{contractId}</p>
      </div>

      <div className="grid-2">
        <div className="card">
          <h2>Event Count (24h)</h2>
          {statsLoading ? (
            <div className="loading">Loading chart...</div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={stats}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="timestamp" />
                <YAxis />
                <Tooltip />
                <Area type="monotone" dataKey="count" stroke="#8b5cf6" fill="#8b5cf6" />
              </AreaChart>
            </ResponsiveContainer>
          )}
        </div>

        <div className="card">
          <h2>Event Type Distribution</h2>
          {chartData.length === 0 ? (
            <div className="empty-state">No events yet</div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={chartData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, value }) => `${name} (${value})`}
                  outerRadius={100}
                  fill="#8b5cf6"
                  dataKey="value"
                >
                  {chartData.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      <div className="card">
        <div className="card-header">
          <h2>Latest Events</h2>
          <Link to={`/explore?contract_id=${contractId}`} className="link">
            View all →
          </Link>
        </div>

        {eventsLoading ? (
          <div className="loading">Loading events...</div>
        ) : events.length === 0 ? (
          <div className="empty-state">No events found</div>
        ) : (
          <table className="events-table">
            <thead>
              <tr>
                <th>Type</th>
                <th>Ledger</th>
                <th>Timestamp</th>
                <th>Tx Hash</th>
              </tr>
            </thead>
            <tbody>
              {events.map((e) => (
                <tr key={e.id}>
                  <td className="event-type">{e.event_type}</td>
                  <td>{e.ledger}</td>
                  <td>{new Date(e.created_at).toLocaleString()}</td>
                  <td className="tx-hash">{e.tx_hash?.slice(0, 16)}...</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
