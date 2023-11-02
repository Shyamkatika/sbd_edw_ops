with 
SLQNTY_RVI AS(
select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_RVI'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_RVI'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
    (SLQNTY.LQNTY * itmrva.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('RVI','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_RVI'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_RVI')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    (SELECT * FROM {{source('INFORXA','ITEMBL')}} qualify 
            row_number() over (PARTITION BY trim(ITNBR),trim(house) ORDER BY LOADDTS DESC)
            = 1
    ) as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}}   
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) 
    ),
    SLQNTY_AUS as
(select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_AUS'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_AUS'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
    (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('AUS','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_AUS'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_AUS')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} 
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) )
, 

  SLQNTY_WIL AS (select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_WIL'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_WIL'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
    (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('WIL','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_WIL'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_WIL')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} as itembl
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid))
)
,
SLQNTY_CHI as
(select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_CHI'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_CHI'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
    (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('CHI','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_CHI'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_CHI')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} as itembl
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) ) -------
    ,
    SLQNTY_GER as
(select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_GER'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_GER'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
        (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('GER','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_GER'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_GER')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} as itembl
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) )
    ,
    
SLQNTY_MEC as
(select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_MEC'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_MEC'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
        (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('MEV','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_MEC'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_MEC')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} 
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) )
    ,
 
SLQNTY_MAR as
(select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_MAR'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_MAR'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
        (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('MAR','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '~','SLQNTY_MAR'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_MAR')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} 
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) )
    ,
    SLQNTY_MTDSW as
(select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_MTDSW'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_MTDSW'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
        (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('MTDSW','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_MTDSW'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_MTDSW')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} 
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) )
    ,
    SLQNTY_SHB as
(select DISTINCT
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_SHB'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_SHB'
            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
        (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('SHB','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_SHB'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_SHB')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} 
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) )
    ,
SLQNTY_TUP as
(select
    upper(
        concat(
            coalesce(trim(slqnty.itnbr)::varchar, ''),
            '~',
            coalesce(trim(slqnty.house)::varchar, ''),
            '~',
            coalesce(trim(slqnty.llocn)::varchar, ''),
            '~','SLQNTY_TUP'
        )
    ) as invty_on_hand_snapshot_key,
    'INFORXA' as SRC_SYS_KEY,
    null as SRC_RCRD_CREATE_DTE,
    null as SRC_RCRD_CREATE_USERID,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as SRC_RCRD_UPD_DTE,
    null as SRC_RCRD_UPD_USERID,
    md5(
        upper(
            concat(
                coalesce(trim(slqnty.itnbr)::varchar, ''),
                '~',
                coalesce(trim(slqnty.house)::varchar, ''),
                '~',
                coalesce(trim(slqnty.llocn)::varchar, ''),
                '~','SLQNTY_TUP'

            )
        )
    ) as RCRD_HASH_KEY,
    'N' as DEL_FROM_SRC_FLAG,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
    current_timestamp::timestamp_ntz as ETL_INS_DTE,
    'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
    current_timestamp::timestamp_ntz as ETL_UPD_DTE,
    slqnty.loaddts as EFF_DTE,
    '9999-12-31 00:00:00.000' as EXPR_DTE,
    'Y' as CURR_RCRD_FLAG,
    'N' as ORP_RCRD_FLAG,
    slqnty.loaddts as ZONE3_LOD_DTE,
    itmrva.b2cocd as cntry_key,
    slqnty.lqnty as prod_on_hand_qty,
    itembl.qtsmo as mtd_sls_units_qty,
    itembl.qtsyr as ytd_sls_units_qty,
    slqnty.lqnty as invty_on_hand_qty,
    itmrva.unmsr as invty_uom_key,
        (SLQNTY.LQNTY * ITMRVA.UCDEF) as AVAIL_INVTY_AMT,
    SLQNTY.LLOCN as STORE_LOC_KEY,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lacdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
            )
        )
    ) as invty_iss_dte,
    -- CURRID.CURID AS CRNCY_KEY,
    slqnty.llocn as loc_5_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(itembl.lphdt), 7, 0), 3),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
            )
        )
    ) as cycle_cnt_dte,
    slqnty.itnbr as prod_key,
    CONCAT('TUP','~',slqnty.house) as loc_key,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as prod_post_dte,
    dateadd(
        year,
        1900,
        try_to_date(
            concat(
                left(lpad(trim(slqnty.fdate), 7, 0), 3),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
                '-',
                substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
            )
        )
    ) as doc_create_dte
from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY_TUP'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
loaddts,lqnty,llocn,house,fdate,itnbr
    from {{source('INFORXA','SLQNTY_TUP')}}) SLQNTY
left join
    {{source('INFORXA','WHSMST')}} as whsmst
    on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))  --{{source('INFORXA','WHSMST')}}
left join
    {{source('INFORXA','ITEMBL')}} as itembl
    on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
    and upper(trim(slqnty.house)) = upper(trim(itembl.house))
left join
    (
        select b2cocd, unmsr, itnbr, stid,UCDEF
        from {{source('INFORXA','ITMRVA')}} 
        qualify
            row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
            = 1
    ) as itmrva
    on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
    and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) )
   
 ,
-- SLQNTY_ONLY AS(
-- select
--     upper(
--         concat(
--             coalesce(trim(slqnty.itnbr)::varchar, ''),
--             '~',
--             coalesce(trim(slqnty.house)::varchar, ''),
--             '~',
--             coalesce(trim(slqnty.llocn)::varchar, ''),
--             '~','SLQNTY'
--         )
--     ) as invty_on_hand_snapshot_key,
--     'INFORXA' as SRC_SYS_KEY,
--     null as SRC_RCRD_CREATE_DTE,
--     null as SRC_RCRD_CREATE_USERID,
--     dateadd(
--         year,
--         1900,
--         try_to_date(
--             concat(
--                 left(lpad(trim(slqnty.fdate), 7, 0), 3),
--                 '-',
--                 substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
--                 '-',
--                 substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
--             )
--         )
--     ) as SRC_RCRD_UPD_DTE,
--     null as SRC_RCRD_UPD_USERID,
--     md5(
--         upper(
--             concat(
--                 coalesce(trim(slqnty.itnbr)::varchar, ''),
--                 '~',
--                 coalesce(trim(slqnty.house)::varchar, ''),
--                 '~',
--                 coalesce(trim(slqnty.llocn)::varchar, ''),
--                 '~','SLQNTY'
--             )
--         )
--     ) as RCRD_HASH_KEY,
--     'N' as DEL_FROM_SRC_FLAG,
--     'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_INS_PID,
--     current_timestamp::timestamp_ntz as ETL_INS_DTE,
--     'EDW_INFORXA_INVENTORY_ON_HAND_SNAPSHOT_VW' as ETL_UPD_PID,
--     current_timestamp::timestamp_ntz as ETL_UPD_DTE,
--     slqnty.loaddts as EFF_DTE,
--     '9999-12-31 00:00:00.000' as EXPR_DTE,
--     'Y' as CURR_RCRD_FLAG,
--     'N' as ORP_RCRD_FLAG,
--     slqnty.loaddts as ZONE3_LOD_DTE,
--     itmrva.b2cocd as cntry_key,
--     slqnty.lqnty as prod_on_hand_qty,
--     itembl.qtsmo as mtd_sls_units_qty,
--     itembl.qtsyr as ytd_sls_units_qty,
--     slqnty.lqnty as invty_on_hand_qty,
--     itmrva.unmsr as invty_uom_key,
--     dateadd(
--         year,
--         1900,
--         try_to_date(
--             concat(
--                 left(lpad(trim(itembl.lacdt), 7, 0), 3),
--                 '-',
--                 substring(lpad(trim(itembl.lacdt), 7, 0), 4, 2),
--                 '-',
--                 substring(lpad(trim(itembl.lacdt), 7, 0), 6, 2)
--             )
--         )
--     ) as invty_iss_dte,
--     -- CURRID.CURID AS CRNCY_KEY,
--     slqnty.llocn as loc_5_key,
--     dateadd(
--         year,
--         1900,
--         try_to_date(
--             concat(
--                 left(lpad(trim(itembl.lphdt), 7, 0), 3),
--                 '-',
--                 substring(lpad(trim(itembl.lphdt), 7, 0), 4, 2),
--                 '-',
--                 substring(lpad(trim(itembl.lphdt), 7, 0), 6, 2)
--             )
--         )
--     ) as cycle_cnt_dte,
--     slqnty.itnbr as prod_key,
--     slqnty.house as loc_key,
--     dateadd(
--         year,
--         1900,
--         try_to_date(
--             concat(
--                 left(lpad(trim(slqnty.fdate), 7, 0), 3),
--                 '-',
--                 substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
--                 '-',
--                 substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
--             )
--         )
--     ) as prod_post_dte,
--     dateadd(
--         year,
--         1900,
--         try_to_date(
--             concat(
--                 left(lpad(trim(slqnty.fdate), 7, 0), 3),
--                 '-',
--                 substring(lpad(trim(slqnty.fdate), 7, 0), 4, 2),
--                 '-',
--                 substring(lpad(trim(slqnty.fdate), 7, 0), 6, 2)
--             )
--         )
--     ) as doc_create_dte
-- from (select upper(concat(coalesce(trim(itnbr)::varchar, ''),'~',coalesce(trim(house)::varchar, ''),'~',coalesce(trim(llocn)::varchar, '','SLQNTY'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
-- loaddts,lqnty,llocn,house,fdate,itnbr
--     from TEST_RAW.INFORXA.VW_SLQNTY) SLQNTY
-- left join
--     TEST_RAW.INFORXA.VW_WHSMST as whsmst
--     on upper(trim(slqnty.house)) = upper(trim(whsmst.whid))
-- left join
--     (SELECT * FROM TEST_RAW.INFORXA.VW_ITEMBL qualify 
--             row_number() over (PARTITION BY trim(ITNBR),trim(house) ORDER BY LOADDTS DESC)
--             = 1
--     ) as itembl
--     on upper(trim(slqnty.itnbr)) = upper(trim(itembl.itnbr))
--     and upper(trim(slqnty.house)) = upper(trim(itembl.house))
-- left join
--     (
--         select b2cocd, unmsr, itnbr, stid
--         from TEST_RAW.INFORXA.VW_ITMRVA as itembl  
--         qualify
--             row_number() over (PARTITION BY trim(ITNBR),trim(STID) ORDER BY ITRV,CHDE DESC)
--             = 1
--     ) as itmrva
--     on upper(trim(slqnty.itnbr)) = upper(trim(itmrva.itnbr))
--     and upper(trim(whsmst.stid)) = upper(trim(itmrva.stid)) 
--     )
-- ,
SLQNTY as(
select * from SLQNTY_RVI
union 
select * from SLQNTY_WIL
UNION 
select * from SLQNTY_CHI
UNION 
SELECT * FROM SLQNTY_GER
UNION
SELECT * FROM SLQNTY_MEC
UNION
SELECT * FROM SLQNTY_MAR
UNION
SELECT * FROM SLQNTY_MTDSW
UNION
SELECT * FROM SLQNTY_SHB
UNION
SELECT * FROM SLQNTY_TUP
UNION
SELECT  * FROM SLQNTY_AUS
-- UNION ALL
-- SELECT * FROM SLQNTY_ONLY
)
SELECT * FROM SLQNTY
