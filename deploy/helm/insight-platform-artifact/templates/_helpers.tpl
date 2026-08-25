{{- define "insight-platform-artifact.name" -}}
{{- printf "insight-platform-artifact-%s" .role -}}
{{- end }}

{{- define "insight-platform-artifact.labels" -}}
app.kubernetes.io/name: insight-platform-artifact
app.kubernetes.io/component: {{ printf "artifact-%s" .role }}
app.kubernetes.io/part-of: insight-platform
insight.platform/workload-role: {{ printf "artifact-%s" .role }}
insight.platform/component-role: {{ printf "artifact_%s" (.role | replace "-" "_") }}
{{- end }}

{{- define "insight-platform-artifact.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
