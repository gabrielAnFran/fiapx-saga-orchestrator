{{- define "saga-orchestrator.envFrom" -}}
envFrom:
  - configMapRef:
      name: {{ .Release.Name }}-config
  - secretRef:
      name: {{ .Release.Name }}-secret
{{- end -}}
