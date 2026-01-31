import streamlit as st
import pandas as pd
import io
import traceback  # 移到顶部
import logic

# CSS: 调大间距，优化字体
st.set_page_config(page_title="审计分摊工具 V0.5", layout="wide", page_icon="🏦")
st.markdown("""
<style>
    .main .block-container {padding-top: 2rem; overflow-y: auto !important;} 
    .stTabs [data-baseweb="tab-list"] { gap: 20px; }
    .stTabs [data-baseweb="tab"] { 
        height: 50px; 
        font-size: 16px; 
        padding: 0 25px;
        background-color: #f8f9fa; 
        border-radius: 8px 8px 0 0; 
    }
    .stTabs [aria-selected="true"] { 
        background-color: #e3f2fd; 
        border-top: 3px solid #1976d2; 
        color: #0d47a1;
        font-weight: bold;
    }
    footer {visibility: hidden;}
    .config-box {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #1976d2;
        margin: 10px 0;
    }
    .success-banner {
        background-color: #e8f5e9;
        border-left: 4px solid #4caf50;
        padding: 10px 15px;
        border-radius: 4px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Session State & Helpers
def init_session_state():
    if 'data_storage' not in st.session_state:
        st.session_state['data_storage'] = {'time': None, 'exp': None, 'wage': None}
    if 'calc_result' not in st.session_state:
        st.session_state['calc_result'] = None
    if 'config_status' not in st.session_state:
        st.session_state['config_status'] = {
            'base_mapped': False,
            'adv_configured': False,
            'mappings': None,
            'labor_subjects': [],
            'clean_data': None,
            'config_error': None  # 用于显示配置错误
        }

def load_data_to_state(file_obj, key):
    if file_obj:
        try:
            # 检查是否是新文件（通过文件名判断）
            current_file_name = getattr(file_obj, 'name', 'unknown')
            last_file_key = f"{key}_last_file"
            
            # 如果是同一个文件，直接跳过（不重置配置）
            if last_file_key in st.session_state and st.session_state[last_file_key] == current_file_name:
                return True
            
            # 新文件，读取数据
            df = pd.read_excel(file_obj)
            if '是否有效' not in df.columns: 
                df.insert(0, '是否有效', True)
            
            st.session_state['data_storage'][key] = df
            st.session_state[last_file_key] = current_file_name  # 记录文件名
            
            # 只有新上传文件时才重置配置状态
            st.session_state['config_status'] = {
                'base_mapped': False,
                'adv_configured': False,
                'mappings': None,
                'labor_subjects': [],
                'clean_data': None,
                'config_error': None
            }
            st.session_state['calc_result'] = None
            return True
            
        except Exception as e:
            st.error(f"加载 {key} 失败: {e}")
            return False
    return False

def render_paginated_editor(key, name):
    df = st.session_state['data_storage'][key]
    if df is None:
        st.info(f"请上传 {name}")
        return
    st.caption(f"当前表格：{name} ({len(df)} 行)")
    st.data_editor(df, key=f"ed_{key}", height=400, hide_index=True, use_container_width=True)

def column_mapper_ui(user_cols, required_fields, optional_fields, key_prefix):
    """字段映射UI，区分必填和选填"""
    mapping = {}
    cols = st.columns(3)
    field_idx = 0
    
    all_fields = [(f, True) for f in required_fields] + [(f, False) for f in optional_fields]
    
    for i, (field, is_required) in enumerate(all_fields):
        default_idx = 0
        for idx, col in enumerate(user_cols):
            if field in col and is_required:
                default_idx = idx
                break
        
        label = f"{field} {'🔴' if is_required else '⚪'}"
        with cols[field_idx % 3]:
            sel = st.selectbox(
                label, 
                [""] + list(user_cols), 
                index=default_idx + 1 if is_required else 0,
                key=f"{key_prefix}_{field}",
                help=f"{'必填项' if is_required else '选填，无数据请留空'}"
            )
            if sel:
                mapping[field] = sel
        field_idx += 1
    
    return mapping

# ==========================================
# Main
# ==========================================
init_session_state()
st.title("🏦 研发费用分摊系统 V0.5 (测试版)")

# Sidebar 数据上传
with st.sidebar:
    st.header("1. 资料上传")
    f_time = st.file_uploader("工时表", type=['xlsx'], key="fu_time")
    f_expense = st.file_uploader("序时账", type=['xlsx'], key="fu_exp")
    f_wage = st.file_uploader("工资表 (可选)", type=['xlsx'], key="fu_wage")
    
    st.divider()
    enable_variance = st.toggle("⚖️ 启用轧差模式", value=True)
    
    if f_time: load_data_to_state(f_time, 'time')
    if f_expense: load_data_to_state(f_expense, 'exp')
    if f_wage: load_data_to_state(f_wage, 'wage')
    
    # 显示配置状态
    st.divider()
    st.header("2. 系统状态")
    status = st.session_state['config_status']
    if status['base_mapped']:
        st.success("✅ 阶段一完成")
        if status['adv_configured']:
            st.success("✅ 阶段二完成")
        else:
            st.warning("⏳ 待阶段二")
    else:
        st.error("⏳ 待阶段一")

# 主界面逻辑
data_ready = (st.session_state['data_storage']['time'] is not None and 
              st.session_state['data_storage']['exp'] is not None)

if not data_ready:
    st.info("👈 请先在侧边栏上传「工时表」和「序时账」")
else:
    cfg = st.session_state['config_status']
    
    tab1, tab2, tab3 = st.tabs(["⚙️ 字段映射配置", "🧹 数据预览与清洗", "🚀 生成审计底稿"])
    
    with tab1:
        # 显示之前的错误（如果有）
        if cfg.get('config_error'):
            st.error(f"上次配置错误: {cfg['config_error']}")
            cfg['config_error'] = None  # 清空错误
        
        # ==========================================
        # 阶段一：基础字段映射 (Form)
        # ==========================================
        if not cfg['base_mapped']:
            st.info("💡 **阶段一：基础字段映射**\n\n请完成必填字段映射，点击底部「确认基础配置」")
            
            with st.form(key="base_mapping_form"):
                st.subheader("1. 工时表映射 (必填)")
                cols_t = st.session_state['data_storage']['time'].columns
                map_t = column_mapper_ui(cols_t, ['工号', '姓名', '月份', '项目号', '工时'], [], 't')
                
                st.subheader("2. 序时账映射 (必填)")
                cols_e = st.session_state['data_storage']['exp'].columns
                map_e = column_mapper_ui(cols_e, ['月份', '项目号', '科目名称', '金额'], [], 'e')
                
                st.subheader("3. 工资表映射 (可选)")
                map_w = {}
                has_wage = st.session_state['data_storage']['wage'] is not None
                
                if has_wage:
                    cols_w = st.session_state['data_storage']['wage'].columns
                    map_w = column_mapper_ui(cols_w, 
                                            ['工号', '姓名', '月份', '工资'], 
                                            ['社保', '公积金', '股份支付'], 
                                            'w')
                else:
                    st.caption("未上传工资表，跳过此项")
                
                submitted = st.form_submit_button("✅ 确认基础配置并加载", type="primary", use_container_width=True)
                
                if submitted:
                    # 验证必填项
                    missing = []
                    for k, v in map_t.items():
                        if not v: missing.append(f"工时表-{k}")
                    for k, v in map_e.items():
                        if not v: missing.append(f"序时账-{k}")
                    
                    if missing:
                        cfg['config_error'] = f"以下必填项未完成映射: {', '.join(missing)}"
                        # 不设置 base_mapped，保持当前页面显示错误
                    else:
                        try:
                            with st.spinner("正在清洗数据..."):
                                d_t = st.session_state['data_storage']['time']
                                d_e = st.session_state['data_storage']['exp']
                                d_w = st.session_state['data_storage']['wage'] if has_wage else None
                                
                                clean_t = logic.clean_data(d_t, map_t)
                                clean_e = logic.clean_data(d_e, map_e)
                                clean_w = logic.clean_data(d_w, map_w) if d_w is not None else None
                                
                                # 保存基础配置状态（关键：不要在这里调 st.rerun()）
                                cfg['base_mapped'] = True
                                cfg['mappings'] = {
                                    'time': map_t, 
                                    'exp': map_e, 
                                    'wage': map_w if has_wage else None,
                                    'has_wage': has_wage
                                }
                                cfg['clean_data'] = {
                                    'time': clean_t, 
                                    'exp': clean_e, 
                                    'wage': clean_w,
                                    'enable_variance': enable_variance
                                }
                                cfg['adv_configured'] = False
                                cfg['labor_subjects'] = []
                                cfg['config_error'] = None
                                
                                # Form 提交后会自动刷新页面，此时 base_mapped=True，会显示阶段二
                                st.success("✅ 基础配置已保存！")
                                st.balloons()
                                
                        except Exception as e:
                            cfg['config_error'] = f"数据清洗失败: {str(e)}"
                            # 保存 traceback 到 session_state 以便显示
                            cfg['error_detail'] = traceback.format_exc()
        
        # ==========================================
        # 阶段二：高级配置（剔除科目）
        # ==========================================
        else:
            # 显示阶段一完成摘要
            st.markdown('<div class="success-banner"><b>✅ 阶段一完成：</b>基础字段映射已保存</div>', unsafe_allow_html=True)
            
            with st.container(border=True):
                st.caption("已配置字段映射：")
                m = cfg['mappings']
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown(f"**工时表：** {', '.join([f'`{k}→{v}`' for k,v in m['time'].items()])}")
                with c2:
                    st.markdown(f"**序时账：** {', '.join([f'`{k}→{v}`' for k,v in m['exp'].items()])}")
                if m.get('has_wage') and m['wage']:
                    st.markdown(f"**工资表：** {', '.join([f'`{k}→{v}`' for k,v in m['wage'].items() if v])}")
            
            # 重置按钮
            if st.button("🔄 重新配置阶段一", type="secondary"):
                cfg['base_mapped'] = False
                cfg['adv_configured'] = False
                st.rerun()  # 这里需要 rerun 来切换回阶段一界面
            
            st.divider()
            
            if not cfg['adv_configured']:
                st.info("💡 **阶段二：序时账人工剔除**\n\n基于已配置的「科目名称」列，选择需要从序时账中剔除的科目")
                
                # 从配置中获取科目名称列，读取可选科目
                subj_col = cfg['mappings']['exp']['科目名称']
                exp_df = st.session_state['data_storage']['exp']
                available_subjects = sorted(exp_df[subj_col].unique().tolist())
                
                st.caption(f"从列 `{subj_col}` 中检测到 {len(available_subjects)} 个不同科目")
                
                # 使用 multiselect 选择剔除科目（不在 form 中，实时更新）
                selected_subjects = st.multiselect(
                    "选择需剔除的科目（工资表已包含的费用，避免重复计算）:",
                    available_subjects,
                    default=cfg.get('labor_subjects', []),  # 保留已选择的
                    help="这些科目的金额将从序时账中剔除"
                )
                
                col1, col2 = st.columns([1, 3])
                with col1:
                    if st.button("✅ 确认剔除设置", type="primary"):
                        cfg['labor_subjects'] = selected_subjects
                        cfg['adv_configured'] = True
                        st.success(f"已保存剔除设置（{len(selected_subjects)} 个科目）")
                        st.rerun()  # 刷新进入完成状态
                        
                with col2:
                    if selected_subjects:
                        st.caption(f"当前选择: {', '.join(selected_subjects)}")
            else:
                st.success("✅ 阶段二完成：剔除科目设置已保存")
                if cfg['labor_subjects']:
                    st.markdown("**已剔除科目：** " + " | ".join([f"`{s}`" for s in cfg['labor_subjects']]))
                else:
                    st.caption("未剔除任何科目（序时账全部保留）")
                
                if st.button("🔄 修改剔除设置"):
                    cfg['adv_configured'] = False
                    st.rerun()
    
    with tab2:
        if not cfg['base_mapped']:
            st.warning("⚠️ 请先完成「阶段一：字段映射配置」")
        else:
            opt = ["工时表", "序时账"]
            if cfg['mappings'].get('has_wage'): 
                opt.append("工资表")
            sel = st.radio("选择要编辑的表格:", opt, horizontal=True)
            
            clean_data = cfg['clean_data']
            
            # 根据选择确定要编辑的数据
            if sel == "工时表": 
                source_key = 'time'
                df_to_edit = clean_data['time'].copy()
            elif sel == "序时账": 
                source_key = 'exp'
                df_to_edit = clean_data['exp'].copy()
            elif sel == "工资表" and clean_data['wage'] is not None: 
                source_key = 'wage'
                df_to_edit = clean_data['wage'].copy()
            else:
                st.stop()
            
            # 显示可交互编辑器
            st.caption(f"💡 提示：可直接双击单元格修改，使用列头筛选/排序，完成后点击底部「保存修改」")
            
            # 关键：使用 data_editor 替代 dataframe
            edited_df = st.data_editor(
                df_to_edit,
                use_container_width=True,
                height=600,  # 拉高点，方便编辑
                hide_index=True,
                key=f"editor_{source_key}",  # 给key才能追踪修改
                num_rows="dynamic",  # 允许增删行（可选）
                column_config={
                    "是否有效": st.column_config.CheckboxColumn(
                        "是否有效",
                        help="取消勾选可标记为无效行（不参与计算）",
                        default=True,
                    )
                } if "是否有效" in df_to_edit.columns else None
            )
            
            # 检测是否有修改
            has_changes = not edited_df.equals(df_to_edit)
            
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("💾 保存修改", type="primary", disabled=not has_changes):
                    # 保存回 clean_data
                    cfg['clean_data'][source_key] = edited_df
                    st.success(f"✅ {sel} 修改已保存！将用于后续计算和导出")
                    st.balloons()
                    
            with col2:
                if has_changes:
                    st.warning("⚠️ 检测到数据有修改，请点击「保存修改」按钮保存变更")
                else:
                    st.caption("未检测到修改")

    with tab3:
        if not cfg['base_mapped']:
            st.warning("⚠️ 请先完成「阶段一：字段映射配置」")
        elif not cfg['adv_configured']:
            st.warning("⚠️ 请先完成「阶段二：剔除科目设置」（在字段映射配置标签页）")
        else:
            st.success("✅ 所有配置已完成，可以生成底稿")
            
            if st.button("🚀 开始分摊计算", type="primary"):
                with st.spinner("正在计算..."):
                    try:
                        d = cfg['clean_data']
                        res = logic.run_allocation_v8(
                            d['wage'], 
                            d['time'], 
                            d['exp'], 
                            cfg['labor_subjects'],
                            d['enable_variance']
                        )
                        st.session_state['calc_result'] = res
                    except Exception as e:
                        st.error(f"计算错误: {str(e)}")
                        st.code(traceback.format_exc())
                    
            res = st.session_state['calc_result']
            if res:
                if 'error' in res:
                    st.error(res['error'])
                else:
                    st.success("计算完成")
                    
                    if 'detected_tracks' in res and res['detected_tracks']:
                        st.caption(f"📊 识别到的人工费赛道: {', '.join(res['detected_tracks'])}")
                    
                    c1, c2 = st.columns([3, 1])
                    with c1: 
                        st.dataframe(res['pivot'], use_container_width=True, height=700)
                    with c2: 
                        st.write("计算日志:")
                        for l in res['logs']:
                            if "❌" in l: st.error(l)
                            elif "✅" in l: st.success(l)
                            elif "⚖️" in l: st.warning(l)
                            else: st.info(l)
                    
                    # 下载
                    if st.button("📥 生成并下载完整审计底稿"):
                        buffer = io.BytesIO()
                        d = cfg['clean_data']
                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                            res['pivot'].to_excel(writer, sheet_name='1_最终分摊表')
                            if not res['agg'].empty:
                                res['agg'].to_excel(writer, sheet_name='2_人工费聚合表', index=False)
                            else:
                                pd.DataFrame({'提示': ['无工资表数据']}).to_excel(writer, sheet_name='2_人工费聚合表', index=False)
                                
                            if not res['detail'].empty:
                                res['detail'].to_excel(writer, sheet_name='3_人工费明细流水', index=False)
                            else:
                                pd.DataFrame({'提示': ['无工资表数据']}).to_excel(writer, sheet_name='3_人工费明细流水', index=False)
                                
                            if not res['orphan'].empty:
                                res['orphan'].to_excel(writer, sheet_name='4_轧差孤儿费用', index=False)
                            else:
                                pd.DataFrame({'提示': ['无孤儿费用']}).to_excel(writer, sheet_name='4_轧差孤儿费用', index=False)
                            
                            if d['wage'] is not None: 
                                d['wage'].to_excel(writer, sheet_name='源_工资表', index=False)
                            d['time'].to_excel(writer, sheet_name='源_工时表', index=False)
                            d['exp'].to_excel(writer, sheet_name='源_序时账', index=False)
                        
                        st.download_button(
                            label="📥 点击下载 Excel", 
                            data=buffer.getvalue(), 
                            file_name="审计底稿.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )