CREATE TABLE insight_platform.conversations (
 tenant_id text NOT NULL, conversation_id text NOT NULL,
 agent_id text NOT NULL, agent_deployment_id text NOT NULL, deployment_digest text NOT NULL CHECK (insight_platform.is_sha256(deployment_digest)),
 input_field text NOT NULL CHECK (octet_length(input_field) BETWEEN 1 AND 128), input_schema_digest text NOT NULL CHECK (insight_platform.is_sha256(input_schema_digest)),
 title text NOT NULL CHECK (octet_length(title) BETWEEN 1 AND 160),
 created_by text NOT NULL CHECK (insight_platform.is_platform_id(created_by) AND left(created_by,4)='prn_'),
 version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
 turn_count integer NOT NULL DEFAULT 0 CHECK (turn_count BETWEEN 0 AND 128),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (tenant_id, conversation_id),
 CHECK (insight_platform.is_platform_id(conversation_id) AND left(conversation_id,4)='cnv_'),
 FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id),
 FOREIGN KEY (tenant_id, agent_id) REFERENCES insight_platform.resources(tenant_id, resource_id),
 FOREIGN KEY (tenant_id, agent_deployment_id) REFERENCES insight_platform.deployments(tenant_id, deployment_id)
);
CREATE INDEX conversations_listing ON insight_platform.conversations(tenant_id, created_at, conversation_id);
CREATE TABLE insight_platform.conversation_turns (
 tenant_id text NOT NULL, conversation_id text NOT NULL, ordinal integer NOT NULL CHECK (ordinal BETWEEN 1 AND 128),
 run_id text NOT NULL, history_through integer NOT NULL CHECK (history_through = ordinal - 1),
 conversation_version bigint NOT NULL CHECK (conversation_version > 1),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (tenant_id, conversation_id, ordinal), UNIQUE (tenant_id, run_id),
 FOREIGN KEY (tenant_id, conversation_id) REFERENCES insight_platform.conversations(tenant_id, conversation_id),
 FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id)
);

CREATE OR REPLACE FUNCTION insight_platform.history_lock_run(p_tenant text,p_run text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $history$
DECLARE root record; snapshot jsonb; cleanup_states jsonb;
BEGIN
    IF p_tenant IS NULL OR p_run IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_run) OR left(p_run,4)<>'run_' THEN RAISE EXCEPTION 'invalid Run identity' USING ERRCODE='22023'; END IF;
    SELECT r.version,r.public_replay_floor,r.public_sequence,r.terminal_at,r.active_work_count,r.history_holds INTO root
        FROM insight_platform.runs r WHERE r.tenant_id=p_tenant AND r.run_id=p_run FOR UPDATE NOWAIT;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(fact)),'[]'::jsonb) INTO cleanup_states FROM (
        SELECT DISTINCT cleanup.state,(cleanup.payload->>'deletion_proof' IS NOT NULL) AS has_deletion_proof
        FROM insight_platform.tasks task JOIN insight_platform.jobs cleanup ON cleanup.tenant_id=task.tenant_id AND cleanup.job_id=task.current_cleanup_job_id
        WHERE task.tenant_id=p_tenant AND task.run_id=p_run ORDER BY cleanup.state,has_deletion_proof LIMIT 32
    ) fact;
    snapshot:=jsonb_build_object('version',root.version,'public_replay_floor',root.public_replay_floor,'public_sequence',root.public_sequence,'terminal_at',root.terminal_at,'active_work_count',root.active_work_count,
        'hold_count',(SELECT count(*) FROM jsonb_object_keys(root.history_holds->'holds')) + (SELECT count(*) FROM insight_platform.conversation_turns WHERE tenant_id=p_tenant AND run_id=p_run),
        'live_jobs',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.run_id=p_run AND j.terminal_at IS NULL LIMIT 1) fact),
        'sandbox_cleanup_obligations',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.run_id=p_run AND j.job_kind='sandbox_capability_execution' AND j.payload#>>'{cleanup,required}' IS DISTINCT FROM 'false' LIMIT 1) fact),
        'live_invocations',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.invocations i WHERE i.tenant_id=p_tenant AND i.run_id=p_run AND i.terminal_at IS NULL LIMIT 1) fact),
        'pending_tasks',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.tasks t WHERE t.tenant_id=p_tenant AND t.run_id=p_run AND t.state='pending' LIMIT 1) fact),
        'cleanup_states',cleanup_states);
    RETURN snapshot;
END $history$;

CREATE OR REPLACE FUNCTION insight_platform.history_lock_owner(p_tenant text,p_id text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE r record; fact jsonb; related text[]:=ARRAY[]::text[]; root_kind text; holds bigint:=0; cleanup_required boolean:=false;
BEGIN
 IF p_tenant IS NULL OR p_id IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_id) THEN RAISE EXCEPTION 'invalid history owner identity' USING ERRCODE='22023'; END IF;
 SELECT state,terminal_at,deadline,active_work_count,history_holds FROM insight_platform.runs WHERE tenant_id=p_tenant AND run_id=p_id FOR UPDATE NOWAIT INTO r;
 IF FOUND THEN
  root_kind:='run'; SELECT (SELECT count(*) FROM jsonb_object_keys(r.history_holds->'holds')) + (SELECT count(*) FROM insight_platform.conversation_turns WHERE tenant_id=p_tenant AND run_id=p_id) INTO holds;
  fact:=jsonb_build_object('state',r.state,'terminal_at',r.terminal_at,'deadline',r.deadline,'active_work',r.active_work_count>0);
 ELSE
  SELECT job_kind,state,terminal_at,deadline,run_id,invocation_id,owner_id,payload#>>'{cleanup,required}' AS cleanup,payload#>>'{physical,cleanup_required}' AS physical_cleanup FROM insight_platform.jobs WHERE tenant_id=p_tenant AND job_id=p_id FOR UPDATE NOWAIT INTO r;
  IF FOUND THEN
   root_kind:='job'; related:=ARRAY[r.run_id,r.invocation_id,r.owner_id]; cleanup_required:=r.cleanup='true' OR r.physical_cleanup='true';
   fact:=jsonb_build_object('state',r.state,'terminal_at',r.terminal_at,'deadline',r.deadline,'active_work',r.terminal_at IS NULL);
  ELSE
   SELECT state,terminal_at,deadline,run_id,owner_id FROM insight_platform.invocations WHERE tenant_id=p_tenant AND invocation_id=p_id FOR UPDATE NOWAIT INTO r;
   IF FOUND THEN
    root_kind:='invocation'; related:=ARRAY[r.run_id,r.owner_id]; fact:=jsonb_build_object('state',r.state,'terminal_at',r.terminal_at,'deadline',r.deadline,'active_work',r.terminal_at IS NULL);
   ELSE
    SELECT state,responded_at,deadline,run_id,invocation_id,owner_id,current_cleanup_job_id FROM insight_platform.tasks WHERE tenant_id=p_tenant AND task_id=p_id FOR UPDATE NOWAIT INTO r;
    IF FOUND THEN
     root_kind:='task'; related:=ARRAY[r.run_id,r.invocation_id,r.owner_id,r.current_cleanup_job_id];
     fact:=jsonb_build_object('state',r.state,'terminal_at',r.responded_at,'deadline',r.deadline,'active_work',r.responded_at IS NULL);
     IF r.current_cleanup_job_id IS NOT NULL THEN
      SELECT (j.state<>'succeeded' OR j.payload->>'deletion_proof' IS NULL) INTO cleanup_required FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.job_id=r.current_cleanup_job_id;
      cleanup_required:=COALESCE(cleanup_required,true);
     END IF;
    ELSE
     SELECT state,source_artifact_id,target_artifact_id,owner_id FROM insight_platform.artifact_links WHERE tenant_id=p_tenant AND artifact_link_id=p_id FOR UPDATE NOWAIT INTO r;
     IF FOUND THEN
      root_kind:='artifact_link'; related:=ARRAY[r.source_artifact_id,r.target_artifact_id,r.owner_id]; fact:=jsonb_build_object('state',r.state,'terminal_at',NULL,'deadline',NULL,'active_work',false);
     ELSE
      SELECT state FROM insight_platform.artifacts WHERE tenant_id=p_tenant AND artifact_id=p_id FOR UPDATE NOWAIT INTO r;
      IF FOUND THEN
       root_kind:='artifact'; fact:=jsonb_build_object('state',r.state,'terminal_at',NULL,'deadline',NULL,'active_work',false);
       SELECT count(*) INTO holds FROM (SELECT 1 FROM insight_platform.artifact_links l WHERE l.tenant_id=p_tenant AND l.target_artifact_id=p_id AND l.link_kind='hold' AND l.state='active' LIMIT 1) held;
      ELSE
       SELECT lifecycle_state FROM insight_platform.resources WHERE tenant_id=p_tenant AND resource_id=p_id FOR UPDATE NOWAIT INTO r;
       IF FOUND THEN root_kind:='resource'; fact:=jsonb_build_object('state',r.lifecycle_state,'terminal_at',NULL,'deadline',NULL,'active_work',false);
       ELSE
        SELECT resource_id FROM insight_platform.resource_versions WHERE tenant_id=p_tenant AND resource_version_id=p_id FOR UPDATE NOWAIT INTO r;
        IF FOUND THEN root_kind:='resource_version'; related:=ARRAY[r.resource_id];
        ELSE
         PERFORM 1 FROM insight_platform.tenant_principals WHERE tenant_id=p_tenant AND principal_id=p_id ORDER BY principal_kind FOR UPDATE NOWAIT;
         IF FOUND THEN root_kind:='tenant_principal';
         ELSE PERFORM 1 FROM insight_platform.tenants WHERE tenant_id=p_tenant AND tenant_id=p_id FOR UPDATE NOWAIT;
          IF FOUND THEN root_kind:='tenant'; ELSE root_kind:='absent'; END IF;
         END IF;
        END IF;
        fact:=jsonb_build_object('state',NULL,'terminal_at',NULL,'deadline',NULL,'active_work',false);
       END IF;
      END IF;
     END IF;
    END IF;
   END IF;
  END IF;
 END IF;
 IF root_kind='absent' THEN
  SELECT resource_id,resource_version_id FROM insight_platform.deployments WHERE tenant_id=p_tenant AND deployment_id=p_id FOR UPDATE NOWAIT INTO r;
  IF FOUND THEN root_kind:='deployment'; related:=ARRAY[r.resource_id,r.resource_version_id];
  ELSE
   PERFORM 1 FROM insight_platform.secret_bindings WHERE tenant_id=p_tenant AND secret_binding_id=p_id FOR UPDATE NOWAIT;
   IF FOUND THEN root_kind:='secret_binding';
   ELSE
    SELECT scope_id FROM insight_platform.quota_accounts WHERE tenant_id=p_tenant AND quota_account_id=p_id FOR UPDATE NOWAIT INTO r;
    IF FOUND THEN root_kind:='quota_account'; related:=ARRAY[r.scope_id];
    ELSE
     SELECT quota_account_id,correlation_id FROM insight_platform.quota_ledger WHERE tenant_id=p_tenant AND quota_entry_id=p_id FOR UPDATE NOWAIT INTO r;
     IF FOUND THEN root_kind:='quota_entry'; related:=ARRAY[r.quota_account_id,r.correlation_id];
     ELSE
      SELECT run_id,artifact_id FROM insight_platform.run_values WHERE tenant_id=p_tenant AND value_id=p_id FOR UPDATE NOWAIT INTO r;
      IF FOUND THEN root_kind:='run_value'; related:=ARRAY[r.run_id,r.artifact_id];
      ELSE
       SELECT run_id FROM insight_platform.run_nodes WHERE tenant_id=p_tenant AND node_id=p_id FOR UPDATE NOWAIT INTO r;
       IF FOUND THEN root_kind:='run_node'; related:=ARRAY[r.run_id]; END IF;
      END IF;
     END IF;
    END IF;
   END IF;
  END IF;
 END IF;
 RETURN fact || jsonb_build_object('schema_version',1,'object_kind',root_kind,'object_id',p_id,'related_ids',to_jsonb(array_remove(related,NULL)),'hold_count',holds,'cleanup_required',COALESCE(cleanup_required,false),
  'live_jobs',EXISTS(SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.owner_id=p_id AND j.terminal_at IS NULL),
  'live_invocations',EXISTS(SELECT 1 FROM insight_platform.invocations i WHERE i.tenant_id=p_tenant AND i.owner_id=p_id AND i.terminal_at IS NULL),
  'pending_tasks',EXISTS(SELECT 1 FROM insight_platform.tasks t WHERE t.tenant_id=p_tenant AND t.owner_id=p_id AND t.responded_at IS NULL));
END $retirement$;
