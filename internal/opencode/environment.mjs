import { mkdirSync, readFileSync, writeFileSync } from "node:fs"
import { createHash } from "node:crypto"
import { dirname, join } from "node:path"

const proofRoot = __PROOF_ROOT__
const ownedKeys = __OWNED_KEYS__
const carrierKey = "acp-go-opencodev2"

export default {
  id: carrierKey,
  setup(ctx) {
    const directory = ctx.location.directory
    mkdirSync(proofRoot, { recursive: true, mode: 0o700 })
    writeFileSync(join(proofRoot, createHash("sha256").update(directory).digest("hex")), directory, { mode: 0o600 })
    ctx.tool.hook("execute.before", async ({ sessionID }) => {
      const endpoint = readFileSync(join(dirname(proofRoot), "endpoint"), "utf8")
      const headers = {
        Authorization: "Basic " + Buffer.from("opencode:" + process.env.OPENCODE_SERVER_PASSWORD).toString("base64"),
        "Content-Type": "application/json",
      }
      let id = sessionID
      const seen = new Set()
      let carrier
      while (id && !seen.has(id) && seen.size < 128) {
        seen.add(id)
        const response = await fetch(endpoint + "/api/session/" + encodeURIComponent(id), { headers, signal: AbortSignal.timeout(10000) })
        if (!response.ok) throw Error("session environment lookup failed")
        const { data } = await response.json()
        const candidate = data.metadata?.[carrierKey]
        if (candidate?.sessionID === data.id) {
          carrier = candidate
          break
        }
        id = data.parentID
      }
      if (!carrier) return
      if (!carrier.env || !Array.isArray(carrier.extraPathDirs)) throw Error("session environment missing")
      const variables = { ...process.env }
      for (const [key, value] of Object.entries(carrier.env)) {
        if (typeof value !== "string") throw Error("invalid session environment")
        if (!key.startsWith("ACP_GO_OPENCODEV2_INTERNAL_")) variables[key] = value
      }
      for (const key of ownedKeys) {
        if (process.env[key] === undefined) delete variables[key]
        else variables[key] = process.env[key]
      }
      const base = carrier.env.PATH ?? variables.PATH ?? ""
      variables.PATH = [...carrier.extraPathDirs, ...base.split(":").filter(Boolean)].join(":")
      const response = await fetch(endpoint + "/api/session/" + encodeURIComponent(sessionID) + "/environment", {
        method: "PUT", headers, body: JSON.stringify({ variables }), signal: AbortSignal.timeout(10000),
      })
      if (!response.ok) throw Error("session environment update failed")
    })
  },
}
