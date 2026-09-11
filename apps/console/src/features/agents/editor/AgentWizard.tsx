import { SchemaFields } from './SchemaFields'
import { Metric } from '../../../shared/ui/console-ui'
import { classNames } from '../../../shared/ui/class-names'
import sharedStyles from '../../../shared/ui/Primitives.module.css'
import styles from '../Agents.module.css'
import type { AgentFormFields } from '../editor'
import type { AgentAuthoringProfile } from '../../../shared/api/types'
import type { AgentExecutionKind, ResolvedAgentBindings } from '../../../shared/compiler/compiler'
const cx = classNames(sharedStyles, styles)

interface Props {
  fields: AgentFormFields
  onChange(patch: Partial<AgentFormFields>): void
  inputSchema: string
  outputSchema: string
  onInputSchemaChange(value: string): void
  onOutputSchemaChange(value: string): void
  profile: AgentAuthoringProfile | null
  exactModel: ResolvedAgentBindings['model']
  existing: boolean
  compiled: boolean
  busy: boolean
  step: number
}
export function AgentWizard({
  fields,
  onChange,
  inputSchema,
  outputSchema,
  onInputSchemaChange,
  onOutputSchemaChange,
  profile,
  exactModel,
  existing,
  compiled,
  busy,
  step,
}: Props) {
  const {
    name,
    displayName,
    executionKind,
    instructions,
    modelAlias,
    classification,
    deadline,
    environment,
    inputSchemaPath,
    outputSchemaPath,
    planPath,
  } = fields
  return (
    <>
      <ol className={cx('wizard-steps')} aria-label="创建步骤">
        {['基本信息', '模型与任务', '输入与输出', '检查与发布'].map((title, index) => (
          <li key={title} aria-current={step === index ? 'step' : undefined}>
            <span>{index + 1}</span>
            {title}
          </li>
        ))}
      </ol>
      <div hidden={step !== 0} className={cx('wizard-step')}>
        <h3>给智能体一个名字</h3>
        <div className={cx('form-grid')}>
          <label>
            <span>名称</span>
            <input
              value={name}
              disabled={Boolean(existing)}
              onChange={(event) => onChange({ name: event.target.value })}
              placeholder="例如 research-assistant"
            />
          </label>
          <label>
            <span>显示名称</span>
            <input
              value={displayName}
              onChange={(event) => onChange({ displayName: event.target.value })}
              placeholder="例如 研究助手"
            />
          </label>
          <label>
            <span>任务类型</span>
            <select
              value={executionKind}
              onChange={(event) => {
                onChange({ executionKind: event.target.value as AgentExecutionKind })
                if (event.target.value === 'deterministic') onOutputSchemaChange(inputSchema)
              }}
            >
              <option value="model_chat">模型对话</option>
              <option value="deterministic">确定性回显</option>
              <option value="full_plan">完整 Plan（高级）</option>
              <option value="framework_graph">框架图（高级）</option>
            </select>
          </label>
        </div>
      </div>
      <div hidden={step !== 1} className={cx('wizard-step')}>
        <h3>告诉智能体要做什么</h3>
        <div className={cx('form-grid')}>
          {executionKind === 'model_chat' && (
            <label className={cx('field--wide')}>
              <span>模型</span>
              <select
                value={modelAlias}
                onChange={(event) => {
                  onChange({ modelAlias: event.target.value })
                }}
              >
                <option value="">选择可用模型</option>
                {exactModel &&
                  !profile?.models.some((model) => model.alias === exactModel.manifest_ref) && (
                    <option value={exactModel.manifest_ref}>
                      {exactModel.manifest_ref}（恢复的精确绑定）
                    </option>
                  )}
                {profile?.models.map((model) => (
                  <option key={model.alias}>{model.alias}</option>
                ))}
              </select>
              {profile && profile.models.length === 0 && (
                <p role="status">暂无可用模型，请先到“模型配置”添加并启用模型。</p>
              )}
            </label>
          )}
          {executionKind !== 'deterministic' ? (
            <label className={cx('field--wide')}>
              <span>任务指令</span>
              <textarea
                rows={7}
                value={instructions}
                onChange={(event) => onChange({ instructions: event.target.value })}
                placeholder="描述角色、任务和期望的回答方式"
              />
            </label>
          ) : (
            <p>将输入按约定结构直接返回，适合检查工作流是否连通。</p>
          )}
        </div>
      </div>
      <div hidden={step !== 2} className={cx('wizard-step stack')}>
        <h3>定义输入与输出</h3>
        <SchemaFields
          label="输入字段"
          source={inputSchema}
          disabled={busy}
          onChange={(next) => {
            onInputSchemaChange(next)
          }}
        />
        <SchemaFields
          label="输出字段"
          source={outputSchema}
          disabled={busy}
          onChange={(next) => {
            onOutputSchemaChange(next)
          }}
        />
      </div>
      <div hidden={step !== 3} className={cx('wizard-step')}>
        <h3>检查配置，然后发布</h3>
        <dl className={cx('metrics')}>
          <Metric label="名称" value={displayName || name} />
          <Metric label="模型" value={modelAlias || '无需模型'} />
          <Metric label="校验状态" value={compiled ? '已通过' : '待校验'} />
        </dl>
        <p className={cx('body-copy')}>
          发布会校验完整配置，并使用当前工作空间权限和模型绑定。发布成功后可立即运行。
        </p>
        <details>
          <summary>高级发布设置</summary>
          <div className={cx('form-grid')}>
            <label>
              <span>数据分类</span>
              <select
                value={classification}
                onChange={(event) => onChange({ classification: event.target.value })}
              >
                {[
                  ['public', '公开'],
                  ['internal', '内部'],
                  ['confidential', '机密'],
                  ['restricted', '受限'],
                ].map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>超时秒数</span>
              <input
                inputMode="numeric"
                value={deadline}
                placeholder={
                  profile ? String(profile.default_deadline_seconds) : '使用工作空间默认值'
                }
                onChange={(event) => onChange({ deadline: event.target.value })}
              />
            </label>
            <label>
              <span>发布环境</span>
              <input
                value={environment}
                placeholder={profile?.default_environment ?? '使用工作空间默认值'}
                onChange={(event) => onChange({ environment: event.target.value })}
              />
            </label>
            <label>
              <span>输入 Schema 路径</span>
              <input
                value={inputSchemaPath}
                onChange={(event) => onChange({ inputSchemaPath: event.target.value })}
              />
            </label>
            <label>
              <span>输出 Schema 路径</span>
              <input
                value={outputSchemaPath}
                onChange={(event) => onChange({ outputSchemaPath: event.target.value })}
              />
            </label>
            {(executionKind === 'full_plan' || executionKind === 'framework_graph') && (
              <label>
                <span>Plan 路径</span>
                <input
                  value={planPath}
                  onChange={(event) => onChange({ planPath: event.target.value })}
                />
              </label>
            )}
          </div>
        </details>
      </div>
    </>
  )
}
