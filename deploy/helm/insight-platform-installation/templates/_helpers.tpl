{{- define "installation.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
insight.platform/installation: {{ .Values.owner | quote }}
{{- end -}}

{{- define "installation.proof" -}}
{{- $root := .root -}}
{{- $proof := .proof -}}
{{- if or (not $proof.job) (not $proof.job_uid) (not $proof.pod) (not $proof.pod_uid) (not $proof.identity_digest) -}}
{{- fail "a completed installation Job and Pod identity is required" -}}
{{- end -}}
{{- $job := lookup "batch/v1" "Job" $root.Release.Namespace $proof.job -}}
{{- $pod := lookup "v1" "Pod" $root.Release.Namespace $proof.pod -}}
{{- if or (not $job) (not $pod) -}}{{ fail "installation proof Job or Pod does not exist" }}{{- end -}}
{{- if or (ne $job.metadata.uid $proof.job_uid) (ne $pod.metadata.uid $proof.pod_uid) (ne (index $job.metadata.annotations "insight.platform/input-digest") $root.Values.plan.input_digest) (ne (index $job.metadata.labels "insight.platform/installation") $root.Values.owner) -}}
{{- fail "installation proof identity differs from the declared installation" -}}{{- end -}}
{{- $completed := false -}}{{- $failed := false -}}
{{- range $job.status.conditions -}}
{{- if and (eq .type "Complete") (eq .status "True") -}}{{- $completed = true -}}{{- end -}}
{{- if and (eq .type "Failed") (eq .status "True") -}}{{- $failed = true -}}{{- end -}}
{{- end -}}
{{- if or $failed (not $completed) (ne (int $job.status.succeeded) 1) (ne (int $job.spec.backoffLimit) 0) -}}{{ fail "installation Job has not completed exactly once" }}{{- end -}}
{{- $owned := false -}}
{{- range $pod.metadata.ownerReferences -}}{{- if and (eq .kind "Job") (eq .uid $proof.job_uid) .controller -}}{{- $owned = true -}}{{- end -}}{{- end -}}
{{- if or (not $owned) (ne $pod.status.phase "Succeeded") (ne (len $pod.spec.containers) 1) (ne (len $pod.status.containerStatuses) 1) -}}{{ fail "installation Pod is not the completed Job's sole container" }}{{- end -}}
{{- $container := first $pod.spec.containers -}}{{- $status := first $pod.status.containerStatuses -}}
{{- $command := index $job.metadata.annotations "insight.platform/phase" -}}
{{- if or (and (eq .phase "prepared") (ne $command "prepare")) (and (eq .phase "provider_started") (ne $command "provider-start")) (and (eq .phase "provider_ready") (ne $command "provider-observe")) (and (eq .phase "ready") (not (has $command (list "provision" "verify")))) -}}{{ fail "proof Job phase is invalid" }}{{- end -}}
{{- $expected := include "installation.command" $command -}}
{{- if or (ne (mustToJson $container.command) (mustToJson (list "/bin/sh" "-ec"))) (ne (mustToJson $container.args) (mustToJson (list $expected))) (ne $container.terminationMessagePath "/tmp/installation-result.json") -}}{{ fail "proof process does not execute the closed installation command" }}{{- end -}}
{{- if or (ne $container.name "installation") (ne $container.image $root.Values.plan.runtime_image) (ne $status.name "installation") (not $status.state.terminated) (ne (int $status.state.terminated.exitCode) 0) -}}{{ fail "installation process did not complete successfully" }}{{- end -}}
{{- $declared := first $job.spec.template.spec.containers -}}
{{- if or (ne (len $job.spec.template.spec.containers) 1) (ne $declared.name $container.name) (ne $declared.image $container.image) (ne (mustToJson $declared.command) (mustToJson $container.command)) (ne (mustToJson $declared.args) (mustToJson $container.args)) -}}{{ fail "proof Pod differs from the declared Job command" }}{{- end -}}
{{- $result := mustFromJson $status.state.terminated.message -}}
{{- if ne (len (regexFindAll `"([^"\\]|\\.)*"[[:space:]]*:` $status.state.terminated.message -1)) 4 -}}{{ fail "installation envelope must have exactly four declared fields" }}{{- end -}}
{{- if or (ne (len $result) 4) (ne (mustToJson $result.schema_version) "1") (ne $result.input_digest $root.Values.plan.input_digest) (ne $result.identity_digest $proof.identity_digest) -}}{{ fail "installation completion envelope does not match the required identity" }}{{- end -}}
{{- if eq .phase "provider_started" -}}
{{- if or (not (has $proof.mode (list "initialize_once" "serve"))) (ne $result.mode $proof.mode) -}}{{ fail "provider start mode differs from the completed owner command" }}{{- end -}}
{{- else -}}
{{- if ne $result.phase .phase -}}{{ fail "installation completion phase differs from the required phase" }}{{- end -}}
{{- end -}}
{{- end -}}


{{- define "installation.command" -}}
{{- if has . (list "provider-start" "provider-observe") -}}
{{- printf "exec /usr/local/bin/platform-installation %s --input /installation-input/input.json --state /installation/private > /tmp/installation-result.json 2>&1" . -}}
{{- else -}}
{{- printf "exec /usr/local/bin/platform-installation %s --input /installation-input/input.json --state /installation/private --output /output%s > /tmp/installation-result.json" . (ternary "" " --binaries /usr/local/bin" (eq . "prepare")) -}}
{{- end -}}
{{- end -}}

{{- define "installation.scheduling" -}}
automountServiceAccountToken: false
nodeSelector:
  kubernetes.io/hostname: {{ .Values.node | quote }}
{{- end -}}

{{- define "installation.outputMounts" -}}
- name: installation-private
  mountPath: /installation
- name: dependency-postgres
  mountPath: /output/dependencies/postgres
- name: dependency-nats
  mountPath: /output/dependencies/nats
- name: dependency-s3
  mountPath: /output/dependencies/s3
- name: dependency-openbao
  mountPath: /output/dependencies/openbao
- name: s3-data
  mountPath: /output/s3-data
- name: openbao-data
  mountPath: /output/openbao-data
- name: nats-data
  mountPath: /output/nats-data
- name: role-console
  mountPath: /output/roles/console
{{- range .Values.plan.processes }}
- name: role-{{ .name }}
  mountPath: /output/roles/{{ .name }}
{{- end }}
- name: installation-input
  mountPath: /installation-input/input.json
  subPath: input.json
  readOnly: true
- name: temporary
  mountPath: /tmp
{{- end -}}

{{- define "installation.outputVolumes" -}}
{{- range list "installation-private" "dependency-postgres" "dependency-nats" "dependency-s3" "dependency-openbao" "s3-data" "openbao-data" "nats-data" "role-console" }}
- name: {{ . }}
  persistentVolumeClaim:
    claimName: {{ . }}
{{- end }}
{{- range .Values.plan.processes }}
- name: role-{{ .name }}
  persistentVolumeClaim:
    claimName: role-{{ .name }}
{{- end }}
- name: installation-input
  configMap:
    name: installation-input
    defaultMode: 0444
- name: temporary
  emptyDir:
    medium: Memory
    sizeLimit: 64Mi
{{- end -}}

{{- define "installation.providerMounts" -}}
- name: installation-private
  mountPath: /installation
- name: installation-input
  mountPath: /installation-input/input.json
  subPath: input.json
  readOnly: true
- name: temporary
  mountPath: /tmp
{{- end -}}

{{- define "installation.providerVolumes" -}}
- name: installation-private
  persistentVolumeClaim:
    claimName: installation-private
- name: installation-input
  configMap:
    name: installation-input
    defaultMode: 0444
- name: temporary
  emptyDir:
    medium: Memory
    sizeLimit: 64Mi
{{- end -}}
