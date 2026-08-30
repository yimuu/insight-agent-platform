import { END, START, StateGraph, StateSchema } from '@langchain/langgraph'
import { z } from 'zod/v4'

const CapabilityState = new StateSchema({
  message: z.string().min(1).max(256),
  result: z.string().default(''),
})

export const capabilityGraph = new StateGraph(CapabilityState)
  .addNode('bounded_prefix', (state) => ({result: `langgraph: ${state.message}`}))
  .addEdge(START, 'bounded_prefix')
  .addEdge('bounded_prefix', END)
  .compile()

export async function invokeCapability(message) {
  const state = await capabilityGraph.invoke({message})
  return state.result
}
