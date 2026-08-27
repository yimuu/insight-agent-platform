{{- define "insight-platform-mcp-host.name" -}}insight-platform-mcp-host{{- end }}
{{- define "insight-platform-mcp-host.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-mcp-host.name" . }}
app.kubernetes.io/component: mcp-host
app.kubernetes.io/part-of: insight-platform
{{- end }}
{{- define "insight-platform-mcp-host.discoveryName" -}}insight-platform-mcp-discovery-worker{{- end }}
{{- define "insight-platform-mcp-host.discoveryLabels" -}}
app.kubernetes.io/name: {{ include "insight-platform-mcp-host.discoveryName" . }}
app.kubernetes.io/component: mcp-discovery-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}
{{- define "insight-platform-mcp-host.image" -}}{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}{{- end }}
{{- define "insight-platform-mcp-host.resourceName" -}}insight-platform-mcp-resource-host{{- end }}
{{- define "insight-platform-mcp-host.resourceLabels" -}}
app.kubernetes.io/name: {{ include "insight-platform-mcp-host.resourceName" . }}
app.kubernetes.io/component: mcp-resource-host
app.kubernetes.io/part-of: insight-platform
{{- end }}
