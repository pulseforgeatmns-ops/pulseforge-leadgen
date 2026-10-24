export type DecisionIntent = 'mission_instruction' | 'approval' | 'rejection' | 'inspection_question' | 'status_check' | 'general_chat' | 'correction' | 'credential_or_access_update' | 'candidate_review' | 'ad_management_request' | 'capability_request' | 'unknown';
export type DecisionRoute = 'mission' | 'approval' | 'inspection' | 'clarification' | 'conversation' | 'identity' | 'session_configuration' | 'specialist' | 'intelligence' | 'unknown';
export interface RoutingDecision {
  intent: DecisionIntent;
  confidence: number;
  mission_bound_probability: number;
  approval_probability: number;
  inspection_probability: number;
  requires_human_clarification: boolean;
  risk_if_misrouted: 'low' | 'medium' | 'high' | 'irreversible';
  recommended_route: DecisionRoute;
}
export interface DecisionState {
  operator_message: string;
  message_truncated: boolean;
  context: Record<string, unknown>;
  recent_messages: Array<{ role: 'operator' | 'max'; text: string }>;
}
export interface ProviderEvaluation {
  decision: RoutingDecision | null;
  model: string | null;
  raw_redacted_response?: unknown;
  fallback_reason?: string;
}
export interface DecisionProvider {
  readonly name: string;
  readonly model: string | null;
  evaluate(state: DecisionState, options?: { signal?: AbortSignal }): Promise<ProviderEvaluation>;
}
