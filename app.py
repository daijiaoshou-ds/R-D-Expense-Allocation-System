import streamlit as st
import pandas as pd
import io
import traceback
import logic

# CSS: 调大间距，优化字体
st.set_page_config(page_title="审计分摊工具 V0.6", layout="wide", page_icon="🏦")
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
            'config_error': None
        }

def load_data_to_state(file_obj, key):
    if file_obj:
        try:
            current_file_name = getattr(file_obj, 'name', 'unknown')
            last_file_key = f"{key}_last_file"
            
            if last_file_key in st.session_state and st.session_state[last_file_key] == current_file_name:
                return True
            
            # 修改：限制从2万改为10万
            MAX_ROWS = 100000
            
            # 先读取文件
            df = pd.read_excel(file_obj)
            
            if len(df) > MAX_ROWS:
                st.error(f"❌ {current_file_name} 行数超过限制（{len(df):,} > {MAX_ROWS:,}），请分批处理或联系管理员")
                return False
            
            if '是否有效' not in df.columns: 
                df.insert(0, '是否有效', True)
            
            st.session_state['data_storage'][key] = df
            st.session_state[last_file_key] = current_file_name
            
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
            # st.success(f"✅ 已加载 {current_file_name}：{len(df):,} 行 × {len(df.columns)} 列")
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
st.title("🏦 研发费用分摊系统 V0.6 (优化版)")

# Sidebar 数据上传
# 在 sidebar 中替换上传部分
with st.sidebar:
    st.header("1. 资料上传")
    
    uploaded_files = st.file_uploader(
        "拖拽或点击上传Excel文件（支持多选）", 
        type=['xlsx', 'xls'], 
        accept_multiple_files=True,
        key="auto_upload"
    )
    
    # 自动识别并加载
    if uploaded_files:
        load_summary = {"工时表": 0, "工资表": 0, "序时账": 0, "跳过": 0}
        
        for file in uploaded_files:
            file_name = file.name
            file_lower = file_name.lower()
            
            # 识别逻辑
            if any(k in file_lower for k in ['工时', 'time', 'hour']):
                key, type_name = 'time', "工时表"
            elif any(k in file_lower for k in ['工资', 'wage', 'salary', '薪酬']):
                key, type_name = 'wage', "工资表"
            elif any(k in file_lower for k in ['序时账', '明细账', 'expense', 'ledger', '研发', '凭证']):
                key, type_name = 'exp', "序时账"
            else:
                # 列名识别兜底
                try:
                    df_preview = pd.read_excel(file, nrows=3)
                    cols = ' '.join(df_preview.columns).lower()
                    
                    if '工时' in cols and '项目' in cols:
                        key, type_name = 'time', "工时表"
                    elif ('工资' in cols or '社保' in cols) and '姓名' in cols:
                        key, type_name = 'wage', "工资表"
                    elif '科目' in cols or '借方' in cols or '摘要' in cols:
                        key, type_name = 'exp', "序时账"
                    else:
                        load_summary["跳过"] += 1
                        continue
                except:
                    load_summary["跳过"] += 1
                    continue
            
            if key and load_data_to_state(file, key):
                load_summary[type_name] += 1
        
        # 极简摘要：一行文字
        if any(v > 0 for k, v in load_summary.items() if k != "跳过"):
            parts = [f"{k}:{v}" for k, v in load_summary.items() if v > 0]
            st.caption(f"✅ 已加载: {' | '.join(parts)}")
        
        if load_summary["跳过"] > 0:
            st.caption(f"⚠️ 跳过 {load_summary['跳过']} 个无法识别的文件")
    
    else:
        st.info("👆 请上传Excel文件（支持多选）")
    
    # 始终显示当前已加载的文件列表（简洁版）
    has_data = any(v is not None for v in st.session_state['data_storage'].values())
    if has_data:
        st.divider()
        st.caption("📁 当前文件：")
        for key, name in [('time', '工时'), ('wage', '工资'), ('exp', '序时账')]:
            df = st.session_state['data_storage'][key]
            if df is not None:
                file_name = st.session_state.get(f"{key}_last_file", "...")
                # 只显示文件名前15个字符
                display_name = file_name[:12] + "..." if len(file_name) > 15 else file_name
                st.text(f"• {name}: {display_name} ({len(df)}行)")
    
    st.divider()
    st.header("2. 孤儿费用处理")
    
    # 简化的孤儿费用控制
    use_funnel = st.toggle("🔄 漏斗轧差（优先归属至最近月份）", value=True)
    use_global = st.toggle("⚡ 暴力轧差（直接全局分摊）", value=False)
    
    # 逻辑判断
    if use_funnel and not use_global:
        orphan_mode = "funnel"
        enable_variance = True
        st.caption("✅ 当前：优先寻找近亲月份归属，无近亲则全局分摊")
    elif use_global and not use_funnel:
        orphan_mode = "global"
        enable_variance = True
        st.caption("✅ 当前：直接按全局工时比例分摊")
    elif use_funnel and use_global:
        orphan_mode = "funnel"  # 优先漏斗
        enable_variance = True
        st.warning("⚠️ 同时开启两种模式，优先使用漏斗模式")
    else:
        orphan_mode = "none"
        enable_variance = False
        st.caption("🚫 当前：不处理孤儿费用（无工时人员的费用将悬空）")
    
    st.divider()
    st.header("3. 系统状态")
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
        if cfg.get('config_error'):
            st.error(f"上次配置错误: {cfg['config_error']}")
            cfg['config_error'] = None
        
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
                    missing = []
                    for k, v in map_t.items():
                        if not v: missing.append(f"工时表-{k}")
                    for k, v in map_e.items():
                        if not v: missing.append(f"序时账-{k}")
                    
                    if missing:
                        cfg['config_error'] = f"以下必填项未完成映射: {', '.join(missing)}"
                    else:
                        try:
                            with st.spinner("正在清洗数据..."):
                                d_t = st.session_state['data_storage']['time']
                                d_e = st.session_state['data_storage']['exp']
                                d_w = st.session_state['data_storage']['wage'] if has_wage else None
                                
                                clean_t = logic.clean_data(d_t, map_t)
                                clean_e = logic.clean_data(d_e, map_e)
                                clean_w = logic.clean_data(d_w, map_w) if d_w is not None else None
                                
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
                                    'enable_variance': enable_variance,
                                    'orphan_mode': orphan_mode
                                }
                                cfg['adv_configured'] = False
                                cfg['labor_subjects'] = []
                                cfg['config_error'] = None
                                
                                st.success("✅ 基础配置已保存！")
                                st.balloons()
                                
                        except Exception as e:
                            cfg['config_error'] = f"数据清洗失败: {str(e)}"
                            cfg['error_detail'] = traceback.format_exc()
        
        else:
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
            
            if st.button("🔄 重新配置阶段一", type="secondary"):
                cfg['base_mapped'] = False
                cfg['adv_configured'] = False
                st.rerun()
            
            st.divider()
            
            if not cfg['adv_configured']:
                st.info("💡 **阶段二：序时账人工剔除**\n\n基于已配置的「科目名称」列，选择需要从序时账中剔除的科目")
                
                subj_col = cfg['mappings']['exp']['科目名称']
                exp_df = st.session_state['data_storage']['exp']
                available_subjects = sorted(exp_df[subj_col].unique().tolist())
                
                st.caption(f"从列 `{subj_col}` 中检测到 {len(available_subjects)} 个不同科目")
                
                selected_subjects = st.multiselect(
                    "选择需剔除的科目（工资表已包含的费用，避免重复计算）:",
                    available_subjects,
                    default=cfg.get('labor_subjects', []),
                    help="这些科目的金额将从序时账中剔除"
                )
                
                col1, col2 = st.columns([1, 3])
                with col1:
                    if st.button("✅ 确认剔除设置", type="primary"):
                        cfg['labor_subjects'] = selected_subjects
                        cfg['adv_configured'] = True
                        st.success(f"已保存剔除设置（{len(selected_subjects)} 个科目）")
                        st.rerun()
                        
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
            
            st.caption(f"💡 提示：可直接双击单元格修改，使用列头筛选/排序，完成后点击底部「保存修改」")
            
            edited_df = st.data_editor(
                df_to_edit,
                use_container_width=True,
                height=600,
                hide_index=True,
                key=f"editor_{source_key}",
                num_rows="dynamic",
                column_config={
                    "是否有效": st.column_config.CheckboxColumn(
                        "是否有效",
                        help="取消勾选可标记为无效行（不参与计算）",
                        default=True,
                    )
                } if "是否有效" in df_to_edit.columns else None
            )
            
            has_changes = not edited_df.equals(df_to_edit)
            
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("💾 保存修改", type="primary", disabled=not has_changes):
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
                            d['enable_variance'],
                            d['orphan_mode']
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
                            
                            # 修改：完善孤儿费用明细表
                            if not res['orphan'].empty:
                                res['orphan'].to_excel(writer, sheet_name='4_孤儿费用明细', index=False)
                            else:
                                pd.DataFrame({'提示': ['无孤儿费用']}).to_excel(writer, sheet_name='4_孤儿费用明细', index=False)
                            
                            # 保留原始数据
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