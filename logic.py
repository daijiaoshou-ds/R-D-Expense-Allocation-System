import pandas as pd
import numpy as np

def clean_data(df, mapping_config, active_col_name='是否有效'):
    """
    通用清洗 - 保留所有原始字段，只增加/重命名标准字段
    """
    if df is None: 
        return None

    # 1. 筛选有效行（保留所有列）
    if active_col_name in df.columns:
        df = df[df[active_col_name] == True].copy()
    else:
        df = df.copy()
        
    # 2. 重命名配置的列（保留原列，增加标准列名）
    # 策略：复制映射的列为标准字段名，不删除原字段
    for standard_name, original_name in mapping_config.items():
        if original_name and original_name in df.columns:
            # 如果标准名和原字段名不同，复制一份
            if standard_name != original_name:
                df[standard_name] = df[original_name]
    
    # 3. 类型处理（针对标准字段名）
    money_cols = ['工资', '社保', '公积金', '股份支付', '金额', '工时']
    for col in money_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    str_cols = ['工号', '姓名', '项目号', '科目名称', '月份']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', '', '<NA>', 'NaN'], np.nan)
            
    if '月份' not in df.columns:
        df['月份'] = '当期'
        
    return df

def run_allocation_v8(df_wage, df_time, df_expense, labor_subjects=[], enable_variance=True):
    """
    V9 核心算法: 正确聚合维度 + 孤儿识别 + 赛道隔离轧差
    """
    logs = []
    
    # 容器
    labor_detail_rows = []   # 人工费分摊明细（宽表）
    orphan_rows = []         # 孤儿费用明细（按赛道）
    final_stream = []        # 最终透视源数据
    
    # ==========================================
    # 动态确定赛道列
    # ==========================================
    track_cols = []
    if df_wage is not None:
        # 只识别标准的人工费赛道（用户配置了的）
        standard_tracks = ['工资', '社保', '公积金', '股份支付']
        track_cols = [c for c in standard_tracks if c in df_wage.columns and df_wage[c].sum() > 0]
        
        if track_cols:
            logs.append(f"ℹ️ 检测到人工费赛道: {track_cols}")
        else:
            logs.append("⚠️ 工资表没有配置标准赛道（工资/社保/公积金/股份支付）")
    else:
        logs.append("ℹ️ 模式: 无工资表，仅序时账分摊")

    # ==========================================
    # Step 1: 工时表聚合（人员+月份+项目维度）
    # ==========================================
    # 确保关键字段存在
    if '姓名' not in df_time.columns or '工号' not in df_time.columns:
        raise ValueError("工时表缺少姓名或工号字段")
    
    # 按 姓名+工号+月份+项目号 聚合（得到每人每月在各项目的工时）
    time_agg = df_time.groupby(['姓名', '工号', '月份', '项目号'])['工时'].sum().reset_index()
    
    # 计算每人每月总工时（用于分摊比例）
    person_month_hours = time_agg.groupby(['姓名', '工号', '月份'])['工时'].sum().reset_index()
    person_month_hours.rename(columns={'工时': '当月总工时'}, inplace=True)
    
    # 全局总工时（用于轧差）
    global_total_hours = time_agg['工时'].sum()
    logs.append(f"📊 全期总工时: {global_total_hours:,.2f} 小时")
    
    # 有效项目列表（序时账直接归集用）
    valid_projects = set(time_agg['项目号'].unique())

    # ==========================================
    # Step 2: 工资表处理（人员+月份维度）
    # ==========================================
    if df_wage is not None and track_cols:
        
        # 工资表聚合：按 姓名+工号+月份 聚合（不分项目，得到每人每月总费用）
        wage_agg = df_wage.groupby(['姓名', '工号', '月份'])[track_cols].sum().reset_index()
        
        # 关联工时：工资表 left join 工时汇总（判断是否有工时记录）
        wage_with_hours = pd.merge(
            wage_agg, 
            person_month_hours, 
            on=['姓名', '工号', '月份'], 
            how='left',
            indicator=True  # 用于判断是否存在工时记录
        )
        
        # 分离正常记录和孤儿
        mask_orphan = (wage_with_hours['_merge'] == 'left_only') | (wage_with_hours['当月总工时'].isna()) | (wage_with_hours['当月总工时'] == 0)
        
        normals = wage_with_hours[~mask_orphan].copy()
        orphans = wage_with_hours[mask_orphan].copy()
        
        logs.append(f"👥 正常分摊记录: {len(normals)} 条，孤儿费用记录: {len(orphans)} 条")
        
        # --- 2.1 正常分摊（有工时的）---
        if not normals.empty:
            for track in track_cols:
                if track not in normals.columns:
                    continue
                    
                total_track = normals[track].sum()
                if total_track == 0:
                    continue
                
                # 计算费率：该赛道金额 / 当月总工时
                normals[f'Rate_{track}'] = normals[track] / normals['当月总工时']
                
                # 炸开到项目明细（关联 time_agg）
                alloc_detail = pd.merge(
                    time_agg[['姓名', '工号', '月份', '项目号', '工时']], 
                    normals[['姓名', '工号', '月份', f'Rate_{track}', track]], 
                    on=['姓名', '工号', '月份'], 
                    how='inner'
                )
                
                # 计算分摊金额
                alloc_detail[f'{track}_分摊'] = alloc_detail['工时'] * alloc_detail[f'Rate_{track}']
                alloc_detail['赛道'] = track
                
                # 存入明细流水（保留原始工资表字段 + 分摊结果）
                # 为了明细表完整，这里关联回原始工资表的其他字段（如果有）
                labor_detail_rows.append(alloc_detail)
                
                # 按项目聚合，进入最终流
                track_proj = alloc_detail.groupby(['月份', '项目号'])[f'{track}_分摊'].sum().reset_index()
                track_proj.rename(columns={f'{track}_分摊': '金额'}, inplace=True)
                track_proj['科目名称'] = track
                final_stream.append(track_proj[['月份', '项目号', '科目名称', '金额']])
        
        # --- 2.2 孤儿费用处理（无工时的）---
        if not orphans.empty:
            for track in track_cols:
                if track not in orphans.columns:
                    continue
                    
                orphan_sum = orphans[track].sum()
                if orphan_sum == 0:
                    continue
                
                # 记录孤儿明细（保留人员信息）
                orphan_rec = orphans[orphans[track] != 0][['姓名', '工号', '月份', track]].copy()
                if not orphan_rec.empty:
                    orphan_rec['赛道'] = track
                    orphan_rec.rename(columns={track: '金额'}, inplace=True)
                    orphan_rows.append(orphan_rec)
                    logs.append(f"⚠️ [{track}] 孤儿费用: {orphan_sum:,.2f} 元（{len(orphan_rec)} 人/月）")
                
                # 轧差分摊（如果启用）
                if enable_variance and global_total_hours > 0:
                    rate = orphan_sum / global_total_hours
                    
                    # 分摊到所有工时记录上（按工时比例）
                    variance_alloc = time_agg[['月份', '项目号', '工时']].copy()
                    variance_alloc['金额'] = variance_alloc['工时'] * rate
                    variance_alloc['科目名称'] = f"{track}-轧差"
                    
                    final_stream.append(variance_alloc[['月份', '项目号', '科目名称', '金额']])
                    logs.append(f"⚖️ [{track}] 轧差完成: {orphan_sum:,.2f} 元 -> 分摊至全期工时")
                else:
                    logs.append(f"🚫 [{track}] 孤儿费用 {orphan_sum:,.2f} 未分摊（轧差禁用或无总工时）")

        # 剔除序时账中的相关科目（避免重复）
        if labor_subjects:
            before_cnt = len(df_expense)
            df_expense = df_expense[~df_expense['科目名称'].isin(labor_subjects)].copy()
            after_cnt = len(df_expense)
            logs.append(f"✂️ 序时账剔除 {before_cnt - after_cnt} 行人工费用科目")

    # ==========================================
    # Step 3: 序时账处理
    # ==========================================
    if df_expense is not None and not df_expense.empty:
        # 标记直接/间接
        df_expense['归集类型'] = df_expense['项目号'].apply(
            lambda x: '直接' if x in valid_projects else '间接'
        )
        
        # 直接费用：项目号明确且在工时表中的
        direct = df_expense[df_expense['归集类型'] == '直接'][['月份', '项目号', '科目名称', '金额']].copy()
        if not direct.empty:
            final_stream.append(direct)
            logs.append(f"📥 序时账直接归集: {len(direct)} 行")
        
        # 间接费用：需要按工时分摊的
        indirect = df_expense[df_expense['归集类型'] == '间接']
        if not indirect.empty:
            # 按月份+科目汇总间接费用池
            indirect_pool = indirect.groupby(['月份', '科目名称'])['金额'].sum().reset_index()
            
            # 每月的工时占比（作为分摊动因）
            month_hours = time_agg.groupby('月份')['工时'].sum().reset_index()
            month_hours.rename(columns={'工时': '当月工时合计'}, inplace=True)
            
            # 关联工时占比
            alloc_indirect = pd.merge(indirect_pool, month_hours, on='月份', how='left')
            
            # 炸开到项目（按项目工时占比）
            time_month_proj = time_agg.groupby(['月份', '项目号'])['工时'].sum().reset_index()
            time_month_proj = pd.merge(time_month_proj, month_hours, on='月份', how='left')
            time_month_proj['分摊比例'] = time_month_proj['工时'] / time_month_proj['当月工时合计']
            
            # 关联分摊
            indirect_allocated = pd.merge(
                indirect_pool, 
                time_month_proj[['月份', '项目号', '分摊比例']], 
                on='月份', 
                how='left'
            )
            indirect_allocated['金额'] = indirect_allocated['金额'] * indirect_allocated['分摊比例']
           # indirect_allocated['科目名称'] = indirect_allocated['科目名称'] + '-分摊'
            
            final_stream.append(indirect_allocated[['月份', '项目号', '科目名称', '金额']])
            logs.append(f"📊 序时账间接分摊: {len(indirect)} 行 -> 按当月工时占比分摊")

    # ==========================================
    # Step 4: 生成输出表
    # ==========================================
    if not final_stream:
        return {'error': '无有效分摊数据', 'logs': logs}
        
    # 4.1 最终分摊透视表
    df_final = pd.concat(final_stream, ignore_index=True)
    pivot = df_final.pivot_table(
        index='项目号', 
        columns='科目名称', 
        values='金额', 
        aggfunc='sum', 
        fill_value=0
    )
    
    # 列排序：基础赛道 -> 轧差 -> 其他
    cols = list(pivot.columns)
    priority_base = track_cols
    priority_var = [f"{t}-轧差" for t in track_cols]
    priority_other = [c for c in cols if c not in priority_base and c not in priority_var]
    sorted_cols = [c for c in priority_base if c in cols] + [c for c in priority_var if c in cols] + priority_other
    pivot = pivot[sorted_cols]
    pivot['合计'] = pivot.sum(axis=1)
    
    # 4.2 人工费明细流水（宽表格式）
    detail_wide = pd.DataFrame()
    if labor_detail_rows:
        # 合并所有赛道明细
        detail_concat = pd.concat(labor_detail_rows, ignore_index=True)
        
        # 透视：行=人员项目信息，列=各赛道分摊金额
        # 先合并同一人员项目月份的多行（不同赛道）
        detail_grouped = detail_concat.groupby(['姓名', '工号', '月份', '项目号', '工时'])[track_cols].sum().reset_index()
        detail_wide = detail_grouped
    
    # 4.3 人工费聚合表（按项目+月份）
    agg_table = pd.DataFrame()
    if not detail_wide.empty:
        agg_table = detail_wide.groupby(['月份', '项目号'])[['工时'] + track_cols].sum().reset_index()
    
    # 4.4 孤儿费用表（分赛道）
    orphan_df = pd.DataFrame()
    if orphan_rows:
        orphan_df = pd.concat(orphan_rows, ignore_index=True)

    # ==========================================
    # Step 5: 试算平衡
    # ==========================================
    if df_wage is not None and track_cols:
        input_total = df_wage[track_cols].sum().sum()
        
        # 输出包含：正常分摊 + 轧差
        output_cols = [c for c in pivot.columns if any(t in c for t in track_cols)]
        output_total = pivot[output_cols].sum().sum() if output_cols else 0
        
        diff = input_total - output_total
        if abs(diff) > 0.1:
            logs.append(f"❌ 试算不平衡! 工资表输入: {input_total:,.2f}, 分摊输出: {output_total:,.2f}, 差异: {diff:,.2f}")
        else:
            logs.append(f"✅ 试算平衡! 差异: {diff:.2f}元")

    return {
        'pivot': pivot,
        'agg': agg_table,
        'detail': detail_wide,
        'orphan': orphan_df,
        'logs': logs,
        'detected_tracks': track_cols
    }