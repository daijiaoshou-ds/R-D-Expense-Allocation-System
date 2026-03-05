import pandas as pd
import numpy as np

def clean_data(df, mapping_config, active_col_name='是否有效'):
    """
    通用清洗 - 保留所有原始字段，只增加/重命名标准字段
    优化：减少不必要的字符串操作，提升大文件处理速度
    """
    if df is None: 
        return None

    # 1. 筛选有效行（保留所有列）
    if active_col_name in df.columns:
        df = df[df[active_col_name] == True].copy()
    else:
        df = df.copy()
    
    # 2. 重命名配置的列（保留原列，增加标准列名）
    for standard_name, original_name in mapping_config.items():
        if original_name and original_name in df.columns:
            if standard_name != original_name:
                df[standard_name] = df[original_name]
    
    # 3. 金额列转为数值（必要操作）
    money_cols = ['工资', '社保', '公积金', '股份支付', '金额', '工时']
    for col in money_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 4. 月份列优化处理（向量化，避免逐行正则）
    if '月份' in df.columns:
        # 先转字符串
        months = df['月份'].astype(str).str.strip()
        # 快速替换常见月份后缀（中文）
        months = months.str.replace('月', '', regex=False)
        months = months.str.replace('份', '', regex=False)
        # 转为数值再转回字符串标准化（1和01统一为1）
        months = pd.to_numeric(months, errors='coerce')
        df['月份'] = months.astype('Int64').astype(str)
    
    # 5. 字符串列基础清理
    str_cols = ['工号', '姓名', '项目号', '科目名称']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', '', '<NA>', 'NaN'], np.nan)
            
    return df

def run_allocation_v8(df_wage, df_time, df_expense, labor_subjects=[], enable_variance=True, orphan_mode="funnel"):
    """
    V9 核心算法: 正确聚合维度 + 孤儿识别 + 赛道隔离轧差
    
    orphan_mode: 
        - "funnel": 漏斗模式（先找近亲，无近亲再全局）
        - "global": 暴力模式（直接全局分摊）
        - "none": 不处理孤儿费用
    """
    logs = []
    
    # 容器初始化
    labor_detail_rows = []   # 正常分摊明细（宽表格式）
    orphan_rows = []         # 孤儿费用记录（用于导出）
    final_stream = []        # 最终透视源数据
    global_orphan_records = []  # 无近亲的孤儿（待全局轧差）
    relative_allocation_records = []  # 有近亲的孤儿（已归属）
    variance_detail_rows = []  # 新增：孤儿费用分摊明细（宽表格式）
    
    # ==========================================
    # 动态确定赛道列
    # ==========================================
    track_cols = []
    if df_wage is not None:
        standard_tracks = ['工资', '社保', '公积金', '股份支付']
        track_cols = [c for c in standard_tracks if c in df_wage.columns and df_wage[c].sum() > 0]
        
        if track_cols:
            logs.append(f"ℹ️ 检测到人工费赛道: {track_cols}")
        else:
            logs.append("⚠️ 工资表没有配置标准赛道（工资/社保/公积金/股份支付）")
    else:
        logs.append("ℹ️ 模式: 无工资表，仅序时账分摊")

    # ==========================================
    # Step 1: 工时表聚合
    # ==========================================
    if '姓名' not in df_time.columns or '工号' not in df_time.columns:
        raise ValueError("工时表缺少姓名或工号字段")
    
    time_agg = df_time.groupby(['姓名', '工号', '月份', '项目号'])['工时'].sum().reset_index()
    person_month_hours = time_agg.groupby(['姓名', '工号', '月份'])['工时'].sum().reset_index()
    person_month_hours.rename(columns={'工时': '当月总工时'}, inplace=True)
    
    global_total_hours = time_agg['工时'].sum()
    logs.append(f"📊 全期总工时: {global_total_hours:,.2f} 小时")
    
    valid_projects = set(time_agg['项目号'].unique())

    # ==========================================
    # Step 2: 工资表处理（人员+月份维度）
    # ==========================================
    if df_wage is not None and track_cols:
        
        wage_agg = df_wage.groupby(['姓名', '工号', '月份'])[track_cols].sum().reset_index()
        
        wage_with_hours = pd.merge(
            wage_agg, 
            person_month_hours, 
            on=['姓名', '工号', '月份'], 
            how='left',
            indicator=True
        )
        
        mask_orphan = (wage_with_hours['_merge'] == 'left_only') | \
                      (wage_with_hours['当月总工时'].isna()) | \
                      (wage_with_hours['当月总工时'] == 0)
        
        normals = wage_with_hours[~mask_orphan].copy()
        orphans = wage_with_hours[mask_orphan].copy()
        
        logs.append(f"👥 正常分摊记录: {len(normals)} 条，孤儿记录: {len(orphans)} 条")

        # ==========================================
        # 孤儿费用处理（根据模式选择）
        # ==========================================
        if orphan_mode == "none":
            logs.append("🚫 孤儿费用处理已禁用")
            if not orphans.empty:
                for idx, orphan_row in orphans.iterrows():
                    for track in track_cols:
                        if track in orphan_row and orphan_row[track] != 0:
                            orphan_rows.append(pd.DataFrame([{
                                '姓名': orphan_row['姓名'],
                                '工号': orphan_row['工号'],
                                '月份': str(orphan_row['月份']),
                                '赛道': track,
                                '金额': orphan_row[track],
                                '处理方式': '未处理（无工时）'
                            }]))
                            
        elif orphan_mode == "global":
            # 暴力模式：所有孤儿直接进入全局轧差
            for idx, orphan_row in orphans.iterrows():
                for track in track_cols:
                    if track not in orphan_row or orphan_row[track] == 0:
                        continue
                    global_orphan_records.append({
                        '姓名': orphan_row['姓名'], 
                        '工号': orphan_row['工号'], 
                        '月份': str(orphan_row['月份']), 
                        '赛道': track, 
                        '金额': orphan_row[track]
                    })
            logs.append(f"⚡ 暴力轧差模式：{len(orphans)} 条孤儿记录将进入全局分摊")
            
        else:  # funnel模式
            if not orphans.empty:
                person_time_history = time_agg.groupby(['姓名', '工号', '月份'])['工时'].sum().reset_index()
                person_time_history['month_num'] = pd.to_numeric(person_time_history['月份'], errors='coerce')
                
                for idx, orphan_row in orphans.iterrows():
                    person_name = orphan_row['姓名']
                    person_id = orphan_row['工号']
                    orphan_month_num = pd.to_numeric(orphan_row['月份'], errors='coerce')
                    
                    # 找历史工时（小于孤儿月份）
                    person_history = person_time_history[
                        (person_time_history['姓名'] == person_name) & 
                        (person_time_history['工号'] == person_id) &
                        (person_time_history['month_num'] < orphan_month_num)
                    ].sort_values('month_num', ascending=False)
                    
                    for track in track_cols:
                        if track not in orphan_row or orphan_row[track] == 0:
                            continue
                        
                        orphan_amount = orphan_row[track]
                        has_relative = False
                        
                        if not person_history.empty:
                            nearest_month = str(int(person_history.iloc[0]['月份']))
                            
                            target_hours = time_agg[
                                (time_agg['姓名'] == person_name) & 
                                (time_agg['工号'] == person_id) & 
                                (time_agg['月份'] == nearest_month)
                            ].copy()
                            
                            if not target_hours.empty:
                                total_hours = target_hours['工时'].sum()
                                if total_hours > 0:
                                    # 生成分摊明细
                                    target_hours['分摊金额'] = target_hours['工时'] / total_hours * orphan_amount
                                    
                                    # 生成最终流数据
                                    track_proj = target_hours.groupby(['月份', '项目号'])['分摊金额'].sum().reset_index()
                                    track_proj.rename(columns={'分摊金额': '金额'}, inplace=True)
                                    track_proj['科目名称'] = f"{track}-轧差"  # 统一为轧差列
                                    final_stream.append(track_proj[['月份', '项目号', '科目名称', '金额']])
                                    
                                    # 生成明细宽表行
                                    variance_row = target_hours[['姓名', '工号', '月份', '项目号', '工时']].copy()
                                    for t in track_cols:
                                        variance_row[f'{t}-轧差'] = 0.0
                                    variance_row[f'{track}-轧差'] = target_hours['分摊金额']
                                    variance_detail_rows.append(variance_row)
                                    
                                    # 记录明细
                                    relative_allocation_records.append({
                                        '姓名': person_name,
                                        '工号': person_id,
                                        '孤儿月份': str(int(orphan_month_num)),
                                        '归属月份': nearest_month,
                                        '赛道': track,
                                        '金额': orphan_amount,
                                        '处理方式': f'漏斗轧差(归属至{nearest_month}月)'
                                    })
                                    
                                    # logs.append(f"🔄 [{track}] {person_name} 的 {int(orphan_month_num)}月费用 → 归属至 {nearest_month}月")
                                    has_relative = True
                        
                        if not has_relative:
                            global_orphan_records.append({
                                '姓名': person_name, 
                                '工号': person_id, 
                                '月份': str(int(orphan_month_num)), 
                                '赛道': track, 
                                '金额': orphan_amount
                            })
                            # logs.append(f"⚠️ [{track}] {person_name} 的 {int(orphan_month_num)}月费用 无近亲，待全局轧差")

        # ==========================================
        # 关键：正常分摊（与孤儿处理同级，确保执行）
        # ==========================================
        if not normals.empty:
           # logs.append(f"🔍 开始正常分摊：{len(normals)} 条记录，赛道：{track_cols}")  # 调试日志
            
            for track in track_cols:
                if track not in normals.columns:
                    logs.append(f"⚠️ 赛道 {track} 不在normals中，跳过")
                    continue
                    
                total_track = normals[track].sum()
                # logs.append(f"🔍 赛道 {track} 总额：{total_track}")  # 调试日志
                
                if total_track == 0:
                    continue
                
                # 计算费率
                normals[f'Rate_{track}'] = normals[track] / normals['当月总工时']
                
                # 关联工时明细
                alloc_detail = pd.merge(
                    time_agg[['姓名', '工号', '月份', '项目号', '工时']], 
                    normals[['姓名', '工号', '月份', f'Rate_{track}', track]], 
                    on=['姓名', '工号', '月份'], 
                    how='inner'
                )
                
                # logs.append(f"🔍 {track} 关联后行数：{len(alloc_detail)}")  # 调试日志
                
                if len(alloc_detail) == 0:
                    continue
                
                # 计算分摊金额
                alloc_detail[track] = alloc_detail['工时'] * alloc_detail[f'Rate_{track}']
                alloc_detail['赛道'] = track
                labor_detail_rows.append(alloc_detail)
                
                # 生成透视表数据（关键：这里必须有数据进入final_stream）
                track_proj = alloc_detail.groupby(['月份', '项目号'])[track].sum().reset_index()
                track_proj.rename(columns={track: '金额'}, inplace=True)
                track_proj['科目名称'] = track  # 这里是"工资"不是"工资-轧差"
                
                #logs.append(f"🔍 {track} 透视行数：{len(track_proj)}，科目名称：{track}")  # 调试日志
                
                final_stream.append(track_proj[['月份', '项目号', '科目名称', '金额']])
                # logs.append(f"✅ {track} 正常分摊完成：{track_proj['金额'].sum():.2f} 元")
        
        # ==========================================
        # 全局孤儿轧差（在所有模式处理后执行，除了none模式）
        # ==========================================
        if global_orphan_records and enable_variance and orphan_mode != "none":
            logs.append(f"🔍 开始全局轧差：{len(global_orphan_records)} 条记录")  # 调试日志
            
            orphan_df_temp = pd.DataFrame(global_orphan_records)
            
            for track in track_cols:
                track_orphans = orphan_df_temp[orphan_df_temp['赛道'] == track]
                if track_orphans.empty:
                    continue
                
                orphan_sum = track_orphans['金额'].sum()
                track_orphans_copy = track_orphans.copy()
                track_orphans_copy['处理方式'] = '全局轧差'
                orphan_rows.append(track_orphans_copy)
                
                logs.append(f"⚠️ [{track}] 全局孤儿费用: {orphan_sum:,.2f} 元")
                
                if global_total_hours > 0:
                    rate = orphan_sum / global_total_hours
                    
                    # 透视表数据
                    variance_alloc = time_agg[['月份', '项目号', '工时']].copy()
                    variance_alloc['金额'] = variance_alloc['工时'] * rate
                    variance_alloc['科目名称'] = f"{track}-轧差"
                    final_stream.append(variance_alloc[['月份', '项目号', '科目名称', '金额']])
                    
                    # 明细数据
                    variance_alloc_detail = time_agg[['姓名', '工号', '月份', '项目号', '工时']].copy()
                    variance_alloc_detail[f'{track}-轧差'] = variance_alloc_detail['工时'] * rate
                    for t in track_cols:
                        if t != track:
                            variance_alloc_detail[f'{t}-轧差'] = 0.0
                    variance_detail_rows.append(variance_alloc_detail)
                    
                    logs.append(f"⚖️ [{track}] 全局轧差完成")
                else:
                    logs.append(f"🚫 [{track}] 无法全局轧差（总工时为0）")

        # 剔除序时账中的相关科目
        if labor_subjects:
            before_cnt = len(df_expense)
            df_expense = df_expense[~df_expense['科目名称'].isin(labor_subjects)].copy()
            after_cnt = len(df_expense)
            logs.append(f"✂️ 序时账剔除 {before_cnt - after_cnt} 行人工费用科目")
            
    # ==========================================
    # Step 3: 序时账处理
    # ==========================================
    if df_expense is not None and not df_expense.empty:
        df_expense['归集类型'] = df_expense['项目号'].apply(
            lambda x: '直接' if x in valid_projects else '间接'
        )
        
        direct = df_expense[df_expense['归集类型'] == '直接'][['月份', '项目号', '科目名称', '金额']].copy()
        if not direct.empty:
            final_stream.append(direct)
            logs.append(f"📥 序时账直接归集: {len(direct)} 行")
        
        indirect = df_expense[df_expense['归集类型'] == '间接']
        if not indirect.empty:
            indirect_pool = indirect.groupby(['月份', '科目名称'])['金额'].sum().reset_index()
            
            month_hours = time_agg.groupby('月份')['工时'].sum().reset_index()
            month_hours.rename(columns={'工时': '当月工时合计'}, inplace=True)
            
            time_month_proj = time_agg.groupby(['月份', '项目号'])['工时'].sum().reset_index()
            time_month_proj = pd.merge(time_month_proj, month_hours, on='月份', how='left')
            time_month_proj['分摊比例'] = time_month_proj['工时'] / time_month_proj['当月工时合计']
            
            indirect_allocated = pd.merge(
                indirect_pool, 
                time_month_proj[['月份', '项目号', '分摊比例']], 
                on='月份', 
                how='left'
            )
            indirect_allocated['金额'] = indirect_allocated['金额'] * indirect_allocated['分摊比例']
            
            final_stream.append(indirect_allocated[['月份', '项目号', '科目名称', '金额']])
            logs.append(f"📊 序时账间接分摊: {len(indirect)} 行")

    # ==========================================
    # Step 4: 生成输出表
    # ==========================================
    if not final_stream:
        return {'error': '无有效分摊数据', 'logs': logs}
        
    df_final = pd.concat(final_stream, ignore_index=True)
    pivot = df_final.pivot_table(
        index='项目号', 
        columns='科目名称', 
        values='金额', 
        aggfunc='sum', 
        fill_value=0
    )
    
    # 列排序优化（代码保持不变）...
    cols = list(pivot.columns)
    priority_base = track_cols
    priority_var = [f"{t}-轧差" for t in track_cols]
    priority_other = [c for c in cols if c not in priority_base and c not in priority_var]
    sorted_cols = [c for c in priority_base if c in cols] + [c for c in priority_var if c in cols] + priority_other
    pivot = pivot[sorted_cols]
    pivot['合计'] = pivot.sum(axis=1)
    
    # ==========================================
    # 4.2 人工费明细流水（宽表格式）- 合并正常和孤儿
    # ==========================================
    detail_wide = pd.DataFrame()
    
    # 处理正常分摊明细（转换为宽表）
    normal_wide = pd.DataFrame()
    if labor_detail_rows:
        detail_concat = pd.concat(labor_detail_rows, ignore_index=True)
        group_cols = ['姓名', '工号', '月份', '项目号', '工时']
        # 只保留实际存在的赛道列
        available_track_cols = [c for c in track_cols if c in detail_concat.columns]
        if available_track_cols:
            normal_wide = detail_concat.groupby(group_cols)[available_track_cols].sum().reset_index()

    # 处理孤儿分摊明细（已经是宽表格式）
    variance_wide = pd.DataFrame()
    if variance_detail_rows:
        variance_concat = pd.concat(variance_detail_rows, ignore_index=True)
        # 找出所有轧差列
        variance_cols = [c for c in variance_concat.columns if '-轧差' in c]
        group_cols = ['姓名', '工号', '月份', '项目号', '工时']
        variance_wide = variance_concat.groupby(group_cols)[variance_cols].sum().reset_index()

    # 合并正常和孤儿明细（关键逻辑）
    if not normal_wide.empty and not variance_wide.empty:
        # 两者都有：外连接合并
        detail_wide = pd.merge(normal_wide, variance_wide, on=['姓名', '工号', '月份', '项目号', '工时'], how='outer')
        detail_wide = detail_wide.fillna(0)
    elif not normal_wide.empty:
        detail_wide = normal_wide
    elif not variance_wide.empty:
        detail_wide = variance_wide
    
    # 确保所有列都存在（正常赛道和轧差赛道），缺失的补0
    if not detail_wide.empty:
        for col in track_cols:
            if col not in detail_wide.columns:
                detail_wide[col] = 0.0
            if f'{col}-轧差' not in detail_wide.columns:
                detail_wide[f'{col}-轧差'] = 0.0
        
        # 列排序：基础信息 -> 正常赛道 -> 轧差赛道
        base_cols = ['姓名', '工号', '月份', '项目号', '工时']
        normal_cols = [c for c in track_cols if c in detail_wide.columns]
        variance_cols = [f'{c}-轧差' for c in track_cols if f'{c}-轧差' in detail_wide.columns]
        
        detail_wide = detail_wide[base_cols + normal_cols + variance_cols]

    # 4.3 人工费聚合表（按项目+月份）- 同时包含正常和轧差
    agg_table = pd.DataFrame()
    if not detail_wide.empty:
        agg_cols = ['月份', '项目号']
        # 汇总所有金额列（正常+轧差）
        sum_cols = [c for c in detail_wide.columns if c not in ['姓名', '工号', '月份', '项目号', '工时']]
        agg_table = detail_wide.groupby(agg_cols)[sum_cols].sum().reset_index()

    # 4.4 孤儿费用明细（代码保持不变）...
    orphan_df = pd.DataFrame()
    if relative_allocation_records:
        relative_df = pd.DataFrame(relative_allocation_records)
        orphan_rows.append(relative_df)
    
    if orphan_rows:
        orphan_df = pd.concat(orphan_rows, ignore_index=True)
        if '金额' in orphan_df.columns:
            orphan_df = orphan_df.sort_values(['赛道', '处理方式', '金额'], ascending=[True, True, False])

    # ==========================================
    # Step 5: 试算平衡
    # ==========================================
    if df_wage is not None and track_cols:
        input_total = df_wage[track_cols].sum().sum()
        
        # 输出包含：正常分摊 + 近亲归属 + 轧差
        output_cols = [c for c in pivot.columns if any(t in c for t in track_cols)]
        output_total = pivot[output_cols].sum().sum() if output_cols else 0
        
        diff = input_total - output_total
        unallocated = sum(o['金额'] for o in global_orphan_records) if global_orphan_records else 0
        
        if abs(diff) > 0.1:
            logs.append(f"❌ 试算不平衡! 输入: {input_total:,.2f}, 输出: {output_total:,.2f}, 差异: {diff:,.2f}")
            if unallocated > 0 and orphan_mode == "none":
                logs.append(f"   其中未分摊孤儿费用: {unallocated:,.2f}（孤儿处理已禁用）")
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