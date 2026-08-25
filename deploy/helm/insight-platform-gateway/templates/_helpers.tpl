{{- define "insight-platform-gateway.name" -}}
{{- printf "%s-%s" .root.Release.Name .role | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "insight-platform-gateway.image" -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end -}}

{{- define "insight-platform-gateway.labels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .role }}
{{- end -}}
