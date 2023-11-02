SELECT 
UPPER(concat(COALESCE(trim(lad_det.lad_nbr)::VARCHAR,''),'~',
             COALESCE(trim(lad_det.lad_line)::VARCHAR,''),'~',
             COALESCE(trim(lad_det.lad_site)::VARCHAR,''),'~',
             COALESCE(trim(lad_det.lad_loc)::VARCHAR,''),'~',
             COALESCE(trim(lad_det.lad_part)::VARCHAR,''))) AS INVTY_ALLOC_KEY,
'EXCEL_QADDB' AS SRC_SYS_KEY,
NULL AS {{var('column_SRC_RCRD_CREATE_DTE')}},
NULL AS {{var('column_SRC_RCRD_CREATE_USERID')}},
NULL AS {{var('column_SRC_RCRD_UPD_DTE')}},
NULL AS {{var('column_SRC_RCRD_UPD_USERID')}}, 
md5(UPPER(concat(COALESCE(trim(lad_det.lad_nbr)::VARCHAR,''),'~',
                 COALESCE(trim(lad_det.lad_line)::VARCHAR,''),'~',
                 COALESCE(trim(lad_det.lad_site)::VARCHAR,''),'~',
                 COALESCE(trim(lad_det.lad_loc)::VARCHAR,''),'~',
                 COALESCE(trim(lad_det.lad_part)::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},  
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
lad_det.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
lad_det.LOADDTS AS {{var('column_z3loddtm')}},
--lad_det.lad_domain AS ORG_KEY,
lad_det.lad_dataset AS ALLOC_DATA_SET_NAME,
lad_det.lad_nbr AS SLS_ORD_LN_KEY,
lad_det.lad_line AS LN_ITEM_NBR,
lad_det.lad_site AS ALLOC_SITE_LOC_KEY,
lad_det.lad_loc AS DEST_STORG_BIN_LOC_ID,
lad_det.lad_part AS PROD_KEY,
lad_det.lad_lot AS ALLOC_LOT_NBR,
lad_det.lad_qty_all AS ALLOC_QTY,
lad_det.lad_qty_pick AS PICK_QTY,
lad_det.lad_qty_chg AS ISS_QTY,
lad_det.lad_ref AS STORG_SUB_LOC_ID,
lad_det.lad_ord_site AS ALLOC_ORD_LOC_KEY,
lad_det.lad_dataset AS DATASET_CLS_1_LKEY,
case when length(trim(UPPER((LAD_DET.LAD_ORD_SITE)))) <1 then {{var('default_mapkey')}} 
     when UPPER(LAD_DET.LAD_ORD_SITE) IS NULL then {{var('default_mapkey')}} 
	 else UPPER(LAD_DET.LAD_ORD_SITE) end AS DFLT_SITE_LOC_KEY,
--CASE WHEN  SUBSTR(TR_TYPE,0,3)='ISS' then cast(tr_qty_chg as TIMESTAMP_NTZ(9)) ELSE null END AS INVTY_ISS_MTH,
--lad_det.lad_site AS LOC_KEY,
lad_det.lad_lot AS LOT_NBR,
lad_det.lad_ref AS PAL_ID,
lad_det.lad_ref AS PAL_NBR,
lad_det.lad_site AS PLANT_LOC_KEY,
lad_det.lad_site AS REPL_LOC_KEY,
lad_det.lad_line AS REF_DOC_LN_NBR,
lad_det.lad_loc AS SLOC_KEY,
lad_det.lad_loc AS STORE_LOC_KEY,
lad_det.lad_nbr AS REF_DOC_HDR_NBR,
--PT_MSTR.pt_part	AS DSCTU_PROD_KEY,
CASE WHEN pt_status = 'OB' then pt_part else '' END AS DSCTU_PROD_KEY,
PT_MSTR.pt_origin AS ORIGINATION_CNTRY_KEY,
PT_MSTR.pt_abc	AS PROD_CLS_LKEY,
PT_MSTR.pt_abc	AS PROD_CLS_TYP_LKEY,
PT_MSTR.pt_desc1 AS	INVTY_LN_1_DESC,
PT_MSTR.pt_desc2 AS	INVTY_LN_2_DESC,
PT_MSTR.pt_part_type AS PROCUR_TYP_LKEY,
PT_MSTR.pt_ship_wt AS	PACKED_PROD_WGT,
PT_MSTR.pt_wks_avg::timestamp_ntz AS AVG_DLV_TM,
PT_MSTR.pt_group AS	PROD_GRP_1_LKEY,
PT_MSTR.pt_shelflife::timestamp_ntz AS PROD_LIFE_TM,
PT_MSTR.pt_price AS	PROD_SELL_PRC,
PT_MSTR.pt_price AS	PROD_STD_PRC,
PT_MSTR.pt_net_wt_um AS	SLS_UOM_KEY,
PT_MSTR.pt_net_wt AS PROD_WGT,
CAST(ld_det.ld_date as TIMESTAMP_NTZ(9)) INVTY_CMPLT_DTE,
--PT_MSTR.pt_user1 AS	SVC_CD_LKEY,
CASE WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='00' then 'OLDER MOWER PARTS' 
     WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='01' then 'LIGHTING'                
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='02' then 'MUFFLERS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='03' then 'GEAR BOX / DIFFERENTIAL PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='04' then 'ACTUATORS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='05' then 'PLASTIC / RUBBER PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='06' then 'HARDWARE'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='07' then 'DAMPENERS / SHOCKS / LINKAGE'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='08' then 'FILTERS'     
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='09' then 'STEERING'       
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='10' then 'BRAKE PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='11' then 'BLADES'      
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='12' then 'SPINDLES / SHAFTS'    
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='13' then 'CABLES / ENGINE PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='14' then 'FABRICATED PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='15' then 'BELTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='16' then 'ELECTRICAL PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='17' then 'FUEL SYSTEM PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='18' then 'TIRES'         
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='19' then 'TOOLS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='20' then 'ENGINE OIL / HYD OIL / ADDITIVES'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='21' then 'SEATS / UPHOLSTERY'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='22' then 'PULLEYS / IDLERS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='23' then 'HOSES'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='24' then 'TRANSMISSION / HYDRAULIC COMPONENTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='25' then 'MANUALS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='26' then 'DECALS / PAINTT'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='27' then 'CLUTCH KITS'     
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='28' then 'BEARINGS / BUSHINGS / SPACERS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='29' then 'CHAIN'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='30' then 'RADIATORS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='31' then 'CATCHER BAGS / MISC KITS'     
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='32' then 'GENERATOR PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='33' then 'GOLF PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='34' then 'PRESSURE WASHER PARTS'    
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='35' then 'MDV PARTS'        
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='90' then 'WARRANTY PARTS'     
WHEN SUBSTR(UPPER(PT_USER1), 0, 2)='99' then 'ACCESSORIES'        
WHEN LENGTH(PT_USER1)=0          then 'Missing Service Code' 
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='000' then 'Open'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='01A' then 'Lighting'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='02A' then'Mufflers'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='02B' then 'Misc Muffler Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='03A' then  'Misc Gear Box Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='03B' then  ''
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='03C' then  'Gear Boxes'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='03D' then  'Differentials'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='03E' then  'Axles'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='03F' then  'Shafts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='04A' then  'Actuators'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05A' then  'Fender'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05B' then  'Fuel Tank'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05C' then  'Pulley Cover'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05D' then  'Discharge Chute'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05E' then  'Catcher Chute'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05F' then  'Catcher Lid'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05G' then  'Fuel Cap/Tank Cap'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05H' then  'Carbon Canister'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05I' then  'Anti-Scalp Wheel'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05J' then  'UHMW Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05K' then  'Misc Plastic/Rubber Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05L' then  'Body Panel Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05M' then  'Air Cleaner Canister'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05N' then  'Flex Tube'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05O' then  'Fuel Tank Service Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='05P' then  'Overflow Tank'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06A' then  'Misc Items'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06B' then  'Bolts/U-Bolt'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06C' then  'Nut'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06D' then  'Washer'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06E' then  'Wheel Nut'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06F' then  'Wheel Stud'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06G' then  'Rivet Nut'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06H' then  'Pin'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06I' then  '(removed rubber and moved to 05)'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06J' then  'Spring'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06K' then  'Knob'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06L' then  'Screw'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='06M' then  'Clamp'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='07A' then  'Dampeners'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='07B' then  'Dampener Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='07C' then  'Shocks'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='07D' then  'Linkage'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08A' then  'Engine-Oil'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08B' then  'Air-Main'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08C' then  'Air-Safety'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08D' then  'Vacuum'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08E' then  '"Vapor'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08F' then  'Hydraulic'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08G' then  'Fuel'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08H' then  'Transmission'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08I' then  'Filter Indicator'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='08J' then  'Misc Filter'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='09A' then  'Misc Steering Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='09B' then  'Steering Box'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='10A' then  'Misc Brake Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='10B' then  'Brake Hubs'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11A' then  'Low Sail'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11B' then  'Medium Sail'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11C' then  'High Sail'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11D' then  'Mulch'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11E' then  'Gator'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11F' then  'Catcher'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11G' then  'Extra High Notched'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11H' then  'Heavy Duty'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11I' then  'Sand'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11J' then  'Shredder'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11K' then  'Swing'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11L' then  'Laser Edge'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='11M' then  'Unknown'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='12A' then  'Spindle'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='12B' then  'Idler (remove and move to 22'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='12C' then  'Misc Spindle Parts (Housing, Spacers, Spindle Shaft, Retaining Ring'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='12D' then  'Jackshaft'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='12E' then  'Half-Shaft'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='12F' then  'Driveshaft'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='12G' then  'Shaft'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='13A' then  'Throttle Cable'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='13B' then  'Choke Cable'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='13C' then  'Engine'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='13D' then  'Misc. Engine Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='13E' then  'Misc. Cables'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='13F' then  'Oil Tank Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14A' then  'Misc Fabricated/Cast Part'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14B' then  'Frame'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14C' then  'Engine Guard/Bumper'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14D' then  'Service Deck'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14E' then  'Foot/Floor Pan'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14F' then  'Seat Pan'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14G' then  'Pulley Cover'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14H' then  'Discharge Chute'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14I' then  'Fork'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14J' then  'Instrument Panel'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='14K' then  'ROPS Part'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='15A' then  'Misc Belt'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='15B' then  'Pump Drive'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='15C' then  'Mower Deck'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='15D' then  'CVT Belt'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='15E' then  'Catcher / Blower Belt'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16A' then  'Misc Electrical'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16B' then  'Wire Harness'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16C' then  'Battery Cable'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16D' then  'Switch'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16E' then  'Relay'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16F' then  'Hour Meter'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16G' then  'Solenoid'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16H' then  'Fuel Gauge/Sender'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16I' then  'Hour/Fuel Display'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16J' then  'Electric Throttle'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16K' then  'Headlight/Tail Light'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16L' then  'Misc Guage'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16M' then  'Battery'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16N' then  'Electric Motor'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='16Z' then  'New Zeon'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='17A' then  'Vapor Line'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='17B' then  'Fuel Valve'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='17C' then  'Fuel Line'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='17D' then  'Mechnical Fuel Gauge'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='17E' then  'Misc Fuel Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18A' then  'Front Wheel & Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18B' then  'Front Wheel'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18C' then  'Front Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18D' then  'Rear Wheel & Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18E' then  'Rear Wheel'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18F' then  'Rear Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18G' then  'Gauge Wheels'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18H' then  'UTV Wheel & Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18I' then  'UTV Wheel'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18J' then  'UTV Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='18K' then  'Misc'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='19A' then  'Misc Tools'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='20A' then  'Fuel Additive'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='20B' then  'Engine Oil'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='20C' then  'Hydraulic Oil'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='20D' then  'Lubrizol'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='20E' then  'Sealants'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21A' then  'Seat'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21B' then  'N/A'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21C' then  'Cushion/Pad'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21D' then  'Knob'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21E' then  'Armrest'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21F' then  'Seat Cover'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21G' then  'Slide'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21H' then  'N/A'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21I' then  'Seat Belt'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21J' then  'Misc. Seat Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='21K' then  'Catcher Bags'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='22A' then  'Pulley'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='22B' then  'Idler Pulley'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='23A' then  'Hose'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24A' then  'Hydraulic Pump'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24B' then  'Hydraulic Motor (Wheel Motor)'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24C' then  'Oil Cooler'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24D' then  'Oil Cooler Fan'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24E' then  'Fan/Pulley Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24F' then  'Misc Hydraulic Parts (Tubes here)'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24G' then  'Seal/Seal Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24H' then  'Reservoir'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24I' then  'Cylinder'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24J' then  'Transmission'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='24K' then  'Misc Transmission Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='25A' then  'Operators Manual'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='25B' then  'Parts Manual'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='25C' then  'Service Manual'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='26A' then  'Decal'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='26B' then  'Safety Decal'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='26C' then  'Paint'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='26D' then  'S/N Plate'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='26E' then  'License Plate'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='27A' then  'Clutch Kits'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='28A' then  'Bearing / Cup / Seal'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='28B' then  'Spacers & Bushings'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='28C' then  'Misc'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='29A' then  'Chain'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='30A' then  'Radiator'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='30B' then  'Misc. Radiator Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31A' then  'Catcher/Adapter Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31B' then  'Mulch Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31C' then  'Flex Forks Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31D' then  'Deck Lift Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31E' then  'Light Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31F' then  'Beacon Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31G' then  'Flasher Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31H' then  'Sand Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31I' then  'Steering Ext Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31J' then  'Stripe Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31K' then  'High Vacuum Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31L' then  'Anti-Scalp Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31M' then  'Semi-Pneu Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31N' then  'Trash Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31O' then  'Cup Holder Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31P' then  'Hour Meter Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31Q' then  'Engine Guard Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31R' then  'USB Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31S' then  'Open'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31T' then  'Open'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31U' then  'MDV Attachments'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31V' then  'Open'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31W' then  'Open'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31X' then  'ROPS'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31Y' then  'Catcher Bags'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='31Z' then  'Misc Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='32A' then  'Misc. Generator Part/Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='33A' then  'All Golf Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='34A' then  'All Pressure Washer Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='35A' then  'All MDV Parts'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99A' then  'Catcher/Adapter Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99B' then  'Mulch Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99C' then  'Flex Forks Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99D' then  'Deck Lift Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99E' then  'Light Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99F' then  'Beacon Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99G' then  'Flasher Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99H' then  'Sand Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99I' then  'Steering Ext Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99J' then  'Stripe Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99K' then  'High Vacuum Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99L' then  'Anti-Scalp Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99M' then  'Semi-Pneu Tire'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99N' then  'Trash Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99O' then  'Cup Holder Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99P' then  'Hour Meter Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99Q' then  'Engine Guard Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99R' then  'MDV Attachments'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99S' then  'Power Washer Items'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99T' then  'Arm Rest Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99U' then  'Hitch Kit'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99V' then  ''
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99W' then  ''
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99X' then  'Fluids & Oils'
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99Y' then  ''
WHEN SUBSTR(UPPER(PT_USER1), 0, 3)='99Z' then  'Misc Kit / Marketing Item'
ELSE '' END AS SVC_CD_LKEY,
PT_MSTR.pt_price AS	PROD_UNIT_PRC,
PS_MSTR.ps_scrp_pct AS PROD_SCRAP_PCT,
--PS_MSTR.ps_end AS PROD_EXPR_DTE,
CASE WHEN UPPER(TRIM(PT_MSTR.PT_STATUS)) = 'OB' THEN PT_MOD_DATE ELSE null END AS PROD_EXPR_DTE,
--pt_mstr.pt_status of “OB” then pulling in the pt_mod_date not ps_end. 
--SI_MSTR.si_entity AS ORG_KEY,
CAST(LD_DET.ld_qty_oh AS NUMBER(28,10)) AS ON_HAND_QTY,
LD_DET.ld_lot	AS PROD_SERL_NBR,
--LD_DET.ld_status AS BLKD_QTY,
CASE WHEN UPPER(LD_STATUS) NOT IN ('FGOOD' ,'GOOD') then LD_QTY_OH ELSE 0 END AS BLKD_QTY,
CASE WHEN length(LD_STATUS) IS NOT NULL then 'Y' else 'N' END AS INVTY_STAT_FLAG,
CAST(LD_DET.ld_qty_oh AS NUMBER(28,10))  AS STK_ON_HAND_QTY,
LD_DET.ld_status AS INVTY_STAT_LKEY,
SI_MSTR.SI_DESC AS WHSE_LOC_NAME,
--LOC_MSTR.loc_loc AS SLOC_KEY,
LOC_MSTR.loc_type AS STORG_TYP_LKEY,
PTP_DET.ptp_draw as DRAWING_NBR,
PTP_DET.ptp_sfty_stk as MIN_PROD_QTY,
PTP_DET.ptp_sfty_stk as REPL_ONHAND_QTY,
PTP_DET.ptp_cum_lead as PROC_LEADTIME_DY_CNT,
PTP_DET.ptp_ord_min	as PROD_MIN_ORD_QTY,
PTP_DET.ptp_ord_mult as REPL_MULT_ORD_QTY,
--PTP_DET.ptp_iss_pol	as INVTY_STORG_TYP_LKEY,
case when length(trim(PTP_ISS_POL)) <1 then {{var('default_flag')}} 
     when PTP_ISS_POL  IS NULL then {{var('default_flag')}} 
	 when  UPPER(PTP_ISS_POL) in ('YES','TRUE','1') then 'Y' 
	 when UPPER(PTP_ISS_POL) in ('NO','FALSE','0') then 'N' 
     else {{var('default_flag')}} end as INVTY_STORG_TYP_LKEY,
PTP_DET.ptp_ord_qty	as LOT_SZ_NBR,
PTP_DET.ptp_buyer as PROD_OWN_KEY,
PTP_DET.ptp_routing	as PROD_RTG_FLAG,
PTP_DET.ptp_sfty_stk as SFTY_STK_QTY,
CASE WHEN UPPER(TRIM(ptp_det.ptp_iss_pol)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_iss_pol)) = 'FALSE' THEN {{var('default_n')}} 
	 WHEN UPPER(TRIM(ptp_det.ptp_iss_pol)) is null THEN {{var('default_flag')}}
	 ELSE ptp_det.ptp_iss_pol END AS SHOP_FLOOR_RTG_FLAG,
PTP_DET.ptp_vend as SUPLR_NBR,
PTP_DET.PTP_SFTY_TME as WORKING_DY_CNT,
PTP_DET.PTP_SFTY_TME as WORKING_DYS_CNT,
IN_MSTR.in_site	as LOC_KEY,
IN_MSTR.in_qty_avail as AVAIL_AMT,
IN_MSTR.in_abc as ABC_CD_LKEY,
IN_MSTR.in_wh as WMS_SCHEMA_NAME,
SUBSTR(XXALT_MSTR.XXALT_VALUE,8,3) as PROD_CTGY_LKEY,
--sct_det.sct_cst_tot	as INVTY_PRC,
CASE WHEN UPPER(SCT_SIM) = 'STANDARD' then SCT_CST_TOT else SCT_LBR_LL END as INVTY_PRC,
--sct_det.sct_ovh_tl,sct_lbr_ll	as PROD_OVHD_COST,
CASE WHEN pt_pm_code='P' or pt_pm_code='p' THEN sct_ovh_tl ELSE sct_lbr_ll end as PROD_OVHD_COST,
sct_det.sct_cst_tot	as TOT_PROD_OWNERSHIP_COST,
--sct_det.sct_cst_tot	as PROD_ALLOC_COST,
CASE when UPPER(SCT_SIM) = 'CURRENT' then SCT_CST_TOT  ELSE 0 END as   PROD_ALLOC_COST,
--sct_det.sct_cst_tot	as PROD_COST,
CASE when UPPER(SCT_SIM) = 'CURRENT' then SCT_CST_TOT  ELSE 0 END as   PROD_COST,
sod_det.sod_std_cost as ORD_GRS_PRC,
sod_det.sod_list_pr	as ORD_NET_PRC,
sod_det.sod_um as UOM_LKEY,
ptp_det.ptp_vend as VEND_NBR, --SMES_SAID --210/203
--prh_hist.prh_per_date as PROD_DLV_FLAG,--SMES SAID210
TR_HIST.tr_um as UOM_KEY,
TR_HIST.tr_userid as PROD_DISPATCHER_USERID_KEY,
--TR_HIST.tr_date	as INVTY_RCPT_DTE,
CASE WHEN   SUBSTR(UPPER(TR_TYPE), 0,3)='RCT' then TR_DATE ELSE NULL END as  INVTY_RCPT_DTE,
--TR_HIST.tr_type	as INVTY_RCPT_TYP_LKEY,
CASE WHEN   SUBSTR(UPPER(TR_TYPE), 0,3)='RCT' then TR_TYPE ELSE NULL END as  INVTY_RCPT_TYP_LKEY,
TR_HIST.tr_last_date as LAST_DISPATCH_DTE,
--TR_HIST.tr_qty_chg as MTHLY_CONSUMED_PROD_QTY,
CASE WHEN  SUBSTR(UPPER(TR_TYPE), 0,5)='ISS_WO' then TR_QTY_CHG ELSE 0 END as MTHLY_CONSUMED_PROD_QTY,
--TR_HIST.tr_qty_chg as PREV_YR_INCM_QTY,
CASE WHEN  SUBSTR(UPPER(TR_TYPE), 0,3)='RCT' and YEAR(TR_DATE)= (YEAR(CURRENT_DATE())-1) THEN  TR_QTY_CHG ELSE 0 END as PREV_YR_INCM_QTY,
--TR_HIST.tr_qty_chg as PREV_YR_OUTGOING_QTY,
CASE WHEN  SUBSTR(UPPER(TR_TYPE), 0,3)='ISS' and YEAR(TR_DATE)= (YEAR(CURRENT_DATE())-1) THEN  TR_QTY_CHG ELSE 0 END  as   PREV_YR_OUTGOING_QTY,
CASE WHEN  SUBSTR(UPPER(TR_TYPE), 0,3)='ISS' THEN TR_QTY_CHG ELSE 0 END  as   PROD_DLV_CNT,
TR_HIST.tr_qty_chg as TOT_GOODS_RCPT_QTY,
--TR_HIST.tr_qty_chg as TOT_ISS_INVTY_CNT,
CASE WHEN  SUBSTR(TR_TYPE,0,3)='ISS' then tr_qty_chg ELSE 0 END AS TOT_ISS_INVTY_CNT,
--ald_det.ald_project	AS PROJ_NBR,
loc_mstr.loc_project as PROJ_NBR,
pl_mstr.pl_inv_acct	AS INVTY_ACCT_NBR,
pl_mstr.pl_user1	AS MATL_TYP_LKEY,
pl_mstr.pl_inv_acct	AS ACCT_LKEY,
ptp_det.ptp_ord_mult AS PROD_PUR_QTY,
pt_mstr.pt_status AS PROD_DESIGNATED_LKEY,
si_mstr.si_entity AS ORG_KEY
FROM {{source("EXCEL_QADDB", "LAD_DET")}} AS LAD_DET --1203
LEFT JOIN {{source("EXCEL_QADDB", "PT_MSTR")}} AS PT_MSTR ON  UPPER(TRIM(PT_MSTR.PT_PART)) = UPPER(TRIM(LAD_DET.LAD_PART)) AND UPPER(TRIM(PT_MSTR.PT_DOMAIN)) = 'EXCEL'
LEFT JOIN (SELECT PS_SCRP_PCT,PS_COMP,PS_DOMAIN FROM {{source("EXCEL_QADDB", "PS_MSTR")}} QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(PS_COMP) ORDER BY PS_PAR,PS_START,PS_MOD_DATE ) =1 ) PS_MSTR ON UPPER(TRIM(PT_MSTR.PT_PART)) = UPPER(TRIM(PS_MSTR.PS_COMP)) AND UPPER(TRIM(PS_MSTR.PS_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "PTP_DET")}} AS PTP_DET ON  UPPER(TRIM(PTP_DET.PTP_PART)) = UPPER(TRIM(LAD_DET.LAD_PART)) AND UPPER(TRIM(PTP_DET.PTP_SITE)) = UPPER(TRIM(LAD_DET.LAD_SITE)) AND UPPER(TRIM(PTP_DET.PTP_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "IN_MSTR")}} AS IN_MSTR ON  UPPER(TRIM(IN_MSTR.IN_SITE)) =  UPPER(TRIM(LAD_DET.LAD_SITE)) AND UPPER(TRIM(IN_MSTR.IN_PART)) = UPPER(TRIM(LAD_DET.LAD_PART)) AND  UPPER(TRIM(IN_MSTR.IN_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "SCT_DET")}} SCT_DET ON UPPER(TRIM(SCT_DET.SCT_PART)) = UPPER(TRIM(IN_MSTR.IN_PART)) AND UPPER(TRIM(SCT_DET.SCT_SITE)) = '1' AND UPPER(TRIM(SCT_DET.SCT_SIM))= 'STANDARD' AND UPPER(TRIM(SCT_DET.SCT_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "LD_DET")}} LD_DET ON UPPER(TRIM(LD_DET.LD_SITE)) = UPPER(TRIM(LAD_DET.LAD_SITE)) AND UPPER(TRIM(LD_DET.LD_LOC)) = UPPER(TRIM(LAD_DET.LAD_LOC)) AND UPPER(TRIM(LD_DET.LD_PART)) = UPPER(TRIM(LAD_DET.LAD_PART)) AND UPPER(TRIM(LD_DET.LD_LOT)) = UPPER(TRIM(LAD_DET.LAD_LOT)) AND UPPER(TRIM(LD_DET.LD_REF)) = UPPER(TRIM(LAD_DET.LAD_REF)) AND UPPER(TRIM(LD_DET.LD_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "SI_MSTR")}} AS SI_MSTR ON  UPPER(TRIM(SI_MSTR.SI_SITE)) = UPPER(TRIM(LAD_DET.LAD_SITE)) AND UPPER(TRIM(SI_MSTR.SI_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "SOD_DET")}} AS SOD_DET ON  UPPER(TRIM(SOD_DET.SOD_NBR)) = UPPER(TRIM(LAD_DET.LAD_NBR))   AND UPPER(TRIM(LAD_DET.LAD_LINE)) = UPPER(TRIM(SOD_DET.SOD_LINE::VARCHAR)) AND UPPER(TRIM(LAD_DET.LAD_PART)) = UPPER(TRIM(SOD_DET.SOD_PART)) AND UPPER(TRIM(SOD_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "LOC_MSTR")}} AS LOC_MSTR ON UPPER(TRIM(LD_DET.LD_SITE))  = UPPER(TRIM(LOC_MSTR.LOC_SITE)) AND UPPER(TRIM(LD_DET.LD_LOC)) = UPPER(TRIM(LOC_MSTR.LOC_LOC)) AND UPPER(TRIM(LOC_MSTR.LOC_DOMAIN)) = 'EXCEL'
LEFT JOIN (SELECT * FROM {{source("EXCEL_QADDB", "TR_HIST")}} WHERE UPPER(TRIM(TR_DOMAIN)) = 'EXCEL'
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(tr_batch)),UPPER(TRIM(tr_part)),UPPER(TRIM(tr_site)) ORDER BY LOADDTS,TR_LAST_DATE)=1) TR_HIST ON UPPER(TRIM(TR_HIST.TR_SITE)) = UPPER(TRIM(LD_DET.LD_SITE)) AND UPPER(TRIM(TR_HIST.TR_LOC)) = UPPER(TRIM(LD_DET.LD_LOC)) AND 
UPPER(TRIM(TR_HIST.TR_PART)) = UPPER(TRIM(LD_DET.LD_PART)) AND UPPER(TRIM(TR_HIST.TR_SERIAL)) = UPPER(TRIM(LD_DET.LD_LOT))  
--LEFT JOIN DEV_RAW.EXCEL_QADDB.VW_PRH_HIST AS PRH_HIST ON PRH_NBR = TR_NBR AND PRH_LINE =TR_LINE AND TR_TYPE IN ('ISS-PO','ISS_PRV') AND UPPER(PRH_DOMAIN) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "PL_MSTR")}} AS PL_MSTR ON UPPER(TRIM(PL_MSTR.PL_PROD_LINE)) = UPPER(TRIM(PT_MSTR.PT_PROD_LINE)) AND UPPER(TRIM(PL_MSTR.PL_DOMAIN)) = 'EXCEL'
LEFT JOIN (SELECT xxalt_key,XXALT_VALUE FROM {{source("EXCEL_QADCUST", "XXALT_MSTR")}} --DEV_RAW.EXCEL_QADCUST.VW_XXALT_MSTR
WHERE UPPER(TRIM(XXALT_DOMAIN)) = 'EXCEL' 
AND UPPER(XXALT_DATASET) = 'PRODUCTHIERARCHY' 
AND UPPER(XXALT_REF_TABLE) = 'PT_MSTR' 
AND UPPER(XXALT_REF_FIELD) = 'PT_PART') AS XXALT_MSTR
ON  UPPER(trim(XXALT_MSTR.xxalt_key)) = UPPER(trim(PT_MSTR.pt_part))
WHERE UPPER(TRIM(LAD_DOMAIN)) ='EXCEL'
AND UPPER(TRIM(LAD_DET.LAD_DATASET)) <> 'RPS_DET'