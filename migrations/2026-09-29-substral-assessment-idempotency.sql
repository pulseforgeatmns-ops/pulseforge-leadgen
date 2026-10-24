-- Apply before deploying the intake route. Existing requests without keys are
-- unaffected. A queue item is also the durable notification to the operator.
CREATE UNIQUE INDEX IF NOT EXISTS substral_assessment_request_key
ON agent_actions (client_id, (payload->>'request_key'))
WHERE created_by = 'studio_substral_site'
  AND action_type = 'website_assessment_request';
