{{- define "installation.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
insight.platform/installation: {{ .Values.plan.input.name | quote }}
{{- end -}}

{{- define "installation.scheduling" -}}
automountServiceAccountToken: false
# Generated provider names already include the cluster domain. Resolve them
# before inherited cloud search suffixes can consume the connection deadline.
dnsPolicy: ClusterFirst
dnsConfig:
  options:
    - name: ndots
      value: "1"
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
- name: installation-gates
  mountPath: /output/gates
- name: role-console
  mountPath: /output/roles/console
- name: role-local-identity
  mountPath: /output/roles/local-identity
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
{{- range list "installation-private" "dependency-postgres" "dependency-nats" "dependency-s3" "dependency-openbao" "s3-data" "openbao-data" "nats-data" "role-console" "role-local-identity" "installation-gates" }}
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


{{- define "installation.gateContainer" -}}
- name: wait-installation
  image: {{ .root.Values.plan.runtime_image | quote }}
  command: [/usr/local/bin/platform-installation]
  args: [wait-gate, --input, /installation-input/input.json, --directory, /installation-gates, --phase, {{ .phase }}]
  securityContext:
    runAsUser: 10001
    runAsGroup: 10001
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities: {drop: [ALL]}
  resources:
    requests: {cpu: 10m, memory: 16Mi}
    limits: {cpu: 100m, memory: 64Mi}
  volumeMounts:
    - {name: installation-gates, mountPath: /installation-gates, readOnly: true}
    - {name: installation-input, mountPath: /installation-input/input.json, subPath: input.json, readOnly: true}
{{- end -}}

{{- define "installation.gateVolumes" -}}
- name: installation-gates
  persistentVolumeClaim: {claimName: installation-gates, readOnly: true}
- name: installation-input
  configMap: {name: installation-input, defaultMode: 0444}
{{- end -}}
