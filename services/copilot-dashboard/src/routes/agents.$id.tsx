import { createFileRoute, useRouter } from '@tanstack/react-router'
import { getAgentConfig } from '../config/agents'

export const Route = createFileRoute('/agents/$id')({ component: AgentDetail })

function AgentDetail() {
  const { id } = Route.useParams()
  const router = useRouter()
  const agent = getAgentConfig(id)

  if (!agent) {
    return (
      <div className="min-h-screen bg-gradient-to-b from-slate-900 via-slate-800 to-slate-900">
        <div className="max-w-7xl mx-auto px-6 py-12">
          <button
            onClick={() => router.history.back()}
            className="mb-6 px-4 py-2 bg-slate-800 text-white rounded hover:bg-slate-700 transition-colors"
          >
            ← Back
          </button>
          <div className="bg-red-500/10 border border-red-500/50 rounded-lg p-8 text-center">
            <h2 className="text-2xl font-bold text-red-400 mb-2">Agent Not Found</h2>
            <p className="text-red-300">The requested agent does not exist.</p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-900 via-slate-800 to-slate-900">
      <div className="max-w-7xl mx-auto px-6 py-12">
        {/* Back button */}
        <button
          onClick={() => router.history.back()}
          className="mb-6 px-4 py-2 bg-slate-800 text-white rounded hover:bg-slate-700 transition-colors flex items-center gap-2"
        >
          ← Back to Dashboard
        </button>

        {/* Agent Header */}
        <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-8 mb-6">
          <div className="flex items-start justify-between mb-4">
            <div className="flex-1">
              <h1 className="text-4xl font-bold text-white mb-3">{agent.displayName}</h1>
              <p className="text-gray-300 text-lg leading-relaxed">{agent.description}</p>
            </div>
            <div className="flex items-center gap-2 px-4 py-2 bg-green-500/10 border border-green-500/50 rounded-lg">
              <span className="w-2 h-2 rounded-full bg-green-500"></span>
              <span className="text-green-400 font-medium">Available</span>
            </div>
          </div>

          <div className="flex gap-4 mb-6">
            <a
              href={agent.webUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="px-6 py-3 bg-cyan-500 text-white rounded-lg hover:bg-cyan-600 transition-colors font-semibold"
            >
              Open Agent Service →
            </a>
            <a
              href={agent.baseUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="px-6 py-3 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition-colors"
            >
              View Base URL
            </a>
          </div>

          <div className="pt-4 border-t border-slate-700">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <div className="text-sm text-gray-400 mb-1">Base URL</div>
                <div className="text-white font-mono text-sm bg-slate-900/50 px-3 py-2 rounded">
                  {agent.baseUrl}
                </div>
              </div>
              <div>
                <div className="text-sm text-gray-400 mb-1">Web UI URL</div>
                <div className="text-white font-mono text-sm bg-slate-900/50 px-3 py-2 rounded">
                  {agent.webUrl}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Features */}
        <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-8 mb-6">
          <h2 className="text-2xl font-bold text-white mb-6">Features & Capabilities</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {agent.features.map((feature, i) => (
              <div
                key={i}
                className="flex items-start gap-3 bg-slate-900/50 border border-slate-600 rounded-lg p-4"
              >
                <span className="text-cyan-400 text-xl">✓</span>
                <span className="text-gray-200">{feature}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Architecture */}
        <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-8">
          <h2 className="text-2xl font-bold text-white mb-6">Architecture & Flow</h2>

          <div className="mb-6">
            <h3 className="text-lg font-semibold text-cyan-400 mb-3">{agent.architecture.type}</h3>
            <p className="text-gray-300 leading-relaxed">{agent.architecture.description}</p>
          </div>

          <div className="bg-slate-900/50 border border-slate-600 rounded-lg p-6">
            <h4 className="text-md font-semibold text-white mb-4">Communication Flow</h4>
            <div className="space-y-4">
              <div className="flex items-center gap-4">
                <div className="flex-shrink-0 w-12 h-12 bg-cyan-500/10 border border-cyan-500/30 rounded-lg flex items-center justify-center text-cyan-400 font-bold">
                  1
                </div>
                <div className="flex-1">
                  <div className="font-semibold text-white">User Interface</div>
                  <div className="text-sm text-gray-400">Web-based chat interface powered by ADK</div>
                </div>
              </div>

              <div className="ml-6 border-l-2 border-slate-700 h-8"></div>

              <div className="flex items-center gap-4">
                <div className="flex-shrink-0 w-12 h-12 bg-cyan-500/10 border border-cyan-500/30 rounded-lg flex items-center justify-center text-cyan-400 font-bold">
                  2
                </div>
                <div className="flex-1">
                  <div className="font-semibold text-white">Agent Processing</div>
                  <div className="text-sm text-gray-400">Gemini 3.8 Flash processes requests with specialized tools</div>
                </div>
              </div>

              <div className="ml-6 border-l-2 border-slate-700 h-8"></div>

              <div className="flex items-center gap-4">
                <div className="flex-shrink-0 w-12 h-12 bg-cyan-500/10 border border-cyan-500/30 rounded-lg flex items-center justify-center text-cyan-400 font-bold">
                  3
                </div>
                <div className="flex-1">
                  <div className="font-semibold text-white">A2A Protocol</div>
                  <div className="text-sm text-gray-400">Enables agent-to-agent communication for complex workflows</div>
                </div>
              </div>

              <div className="ml-6 border-l-2 border-slate-700 h-8"></div>

              <div className="flex items-center gap-4">
                <div className="flex-shrink-0 w-12 h-12 bg-cyan-500/10 border border-cyan-500/30 rounded-lg flex items-center justify-center text-cyan-400 font-bold">
                  4
                </div>
                <div className="flex-1">
                  <div className="font-semibold text-white">Response Delivery</div>
                  <div className="text-sm text-gray-400">Results returned to user via chat interface</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
