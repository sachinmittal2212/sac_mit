CREATE OR REPLACE PACKAGE INVEST.PK_IVZ_IFT_EXTRACT is
TYPE RefCursor IS REF CURSOR;

PROCEDURE SP_IVZ_IFT_EXTRACT_POSITION(
o_ReturnRecords IN OUT RefCursor,
i_AccountCode IN VARCHAR2
);

PROCEDURE SP_IVZ_IFT_EXTRACT_TRANSACTION(
o_ReturnRecords IN OUT RefCursor,
i_TranAccountCode IN VARCHAR2
);

PROCEDURE SP_IVZ_IFT_EXTRACT_ACCOUNTAUM(
o_ReturnRecords IN OUT RefCursor,
i_AumAccountCode in VARCHAR2
);


  -- **************************************************************************
  --
  -- Author:  Waseem Mohammed
  -- Date:    23/04/2021
  --
  -- Description:
  --   This package is for Income Forecasting tool which
  --   extracts latest positions, Last 2 years historical positions
  --   Account AUM & All active account details
  --
  -- **************************************************************************

PROCEDURE SP_IVZ_IFT_EXTRACT_ACCOUNTS(
o_ReturnRecords IN OUT RefCursor
);

END PK_IVZ_IFT_EXTRACT;
/

CREATE OR REPLACE PACKAGE BODY PK_IVZ_IFT_EXTRACT IS
  -- **************************************************************************************
  -- This Procedure extracts the latest Positions
  -- Parameters:
  --  i_AccountCode:  Fund code used to retreive latest positions
  -- **************************************************************************************
PROCEDURE SP_IVZ_IFT_EXTRACT_POSITION(
o_ReturnRecords IN OUT RefCursor,
i_AccountCode IN VARCHAR2
) AS

  l_AccountCode varchar2(20);

  BEGIN
  l_AccountCode := i_AccountCode;

OPEN o_ReturnRecords FOR

select position.POSITION_dATE,position.TOTAL_FUND_CODE,position.Country_of_Domicile,position.sector,position.stock_name,position.Instrument_Currency,position.sedol,position.isin,position.ticker,position.ric_code,position.market_value,position.HOLDING,position.mkt_price,/*position.Exch_Rate,*/
round((ratio_to_report(position.market_value) over (partition by position.TOTAL_FUND_CODE)   )*100,3) as Portfolio_Weight
 from (
--SELECT * FROM (
select Position_date,pos.total_fund_code,c.iso_country_code_3char Country_of_Domicile, vicm.ivcg1_display_description sector, iv.issuer || ' ' || iv.issue stock_name,IV.Ccy_Code_Base Instrument_Currency, TRIM(LEADING '0' FROM isl.sedol)sedol,isl.isin,ivs.external_reference ticker,isl.ric_code,NVL(pos.mid_market_value,
                 NVL(pos.off_market_value, pos.bid_market_value)) market_value,
                  pos.mid_position HOLDING,
             pos.mid_price_local mkt_price,
             pos.exch_rate_market Exch_Rate from positions pos join iv_synonym_lookups isl ON pos.iv_id = isl.iv_id
       join investment_vehicles iv on pos.iv_id = iv.id
       left join v_iv_class_members vicm ON vicm.ivcm_iv_id = iv.id AND vicm.ivct_code= 'ICB'
       join countries c on  iv.cou_code_domiciled_in = c.code
       LEFT join iv_synonyms ivs on iv.id = ivs.iv_id  and ivs.syt_code='BLOOM_TICKER' and ivs.effective_end_date > sysdate -1 where
(select to_char(max(position_date),'DD-MON-YYYY') from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%') BETWEEN vicm.ivcm_effective_start_date AND
             vicm.ivcm_effective_end_date
and total_fund_code  = l_AccountCode AND POSITION_DATE =(select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%')
and pos.time= (select max(time) from positions p where total_fund_code  = l_AccountCode and position_Date= (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%'))
AND POS.OFFICIAL_YN = (select  max(official_yn) from positions p where total_fund_code  = l_AccountCode
and position_Date= (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%')
and time = (select max(time) from positions where total_fund_code  = l_AccountCode
and position_Date= (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%')  )  )
and position_Date= (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%')
AND POS.SECID <> 'CASH'
AND NOT(NVL(pos.price_source, 'NONE') = 'BNYM'
             and pos.accrued_interest <> 0 and pos.mid_position = 0)
                      AND NOT ((pos.fmc_code_main LIKE '005%') OR
              ((SUBSTR(pos.fmc_code_main, 1, 3) = '040') OR
              ((SUBSTR(pos.fmc_code_main, 1, 3) >= '010') AND (SUBSTR(pos.fmc_code_alternate, 1, 3) < '010'))) OR
              pos.fmc_code_main = '000000000')

UNION
select distinct cc.position_date,cc.fund_code "TOTAL_FUND_CODE",null "Country_of_Domicile", null sector, 'Cash(Investable)' STOCK_NAME,null Instrument_Currency ,'Cash' SEDOL, 'Cash' ISIN, 'Cash' TICKER, 'Cash' Ric_code, (TRADED_CAPITAL_CASH - UNSETTLED_CAPITAL_LONG + MANAGEMENT_FEES_CAPITAL + AIM_STIC + TERM_DEPOSITS_SHORT + SPOT_FX + CLOSED_FORWARD_FX_SHORT - ESTIMATED_DISTRIBUTIONS + COLLATERAL_RECEIVED ) +
(TRADED_INCOME_CASH  - UNSETTLED_INCOME) "MARKET_VALUE", 0 HOLDING, 0.00 MKT_PRICE, 0 Exch_Rate
from CASH_COMPONENTS cc
         where  cc.fund_code  = l_AccountCode
           AND (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%') = cc.position_date
           AND (select max(time) from positions p where total_fund_code  = l_AccountCode and position_Date= (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%')) = cc.position_time

UNION
select  cc.position_date,cc.fund_code "TOTAL_FUND_CODE",null Country_of_Domicile, null sector, 'Cash(Uninvestable)' STOCK_NAME, null Instrument_Currency ,'Cash' SEDOL, 'Cash' ISIN, 'Cash' TICKER,'Cash' Ric_code, (UNSETTLED_CAPITAL_LONG + TRADED_MARGIN_CASH + DEBENTURE_INCLUDE + TOTAL_LOANS_INCLUDE + TERM_DEPOSITS_LONG + REPOS + PENDING_CASH + CONTRA_FUTURES + CLOSED_FORWARD_FX_LONG + OPEN_FORWARD_FX + UNIDENTIFIED_COMPONENT + ACCRUED_INT_SECURITIES +
ACCRUED_INT_CASH + COLLATERAL_PAID) + UNSETTLED_INCOME "MARKET_VALUE", 0 HOLDING, 0.00 MKT_PRICE,0 Exch_Rate
from CASH_COMPONENTS cc
         where  cc.fund_code  = l_AccountCode
           AND (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%') = cc.position_date
           AND (select max(time) from positions p where total_fund_code  = l_AccountCode and position_Date= (select max(position_date) from positions where total_fund_code  = l_AccountCode AND ISSUER NOT LIKE '%CASH%') ) = cc.position_time
 ) position order by total_fund_code,STOCK_NAME;

END SP_IVZ_IFT_EXTRACT_POSITION;

  -- *********************************************************************************************
  -- This Procedure extracts the dividend transactions paid on the fund in last 2 years
  -- Parameters:
  --  i_TranAccountCode:  Fund code used to retreive last 2 years historical dividend transactions
  -- ************************************************************************************************

PROCEDURE SP_IVZ_IFT_EXTRACT_TRANSACTION(
o_ReturnRecords IN OUT RefCursor,
i_TranAccountCode IN VARCHAR2
) AS

  l_TranAccountCode varchar2(20);
  BEGIN
  l_TranAccountCode := i_TranAccountCode;

OPEN o_ReturnRecords FOR

SELECT
       t.fund_code,
       T.transaction_number,
       extract_time,
       t.ticker,
     TRIM(LEADING '0' FROM iv.sedol)sedol,
       iv.isin,
       t.issuer,
       'Paid' Status,
       shares_par_value           "SHARESPAR",
       t.unconverted_gross_amount "NETAMOUNT",
       t.unconverted_unit_price   "DIVIDENDRATE",
       t.ex_date "EXDATE",
       t.settlement_date          "PAYDATE",
       t.ccy_code_price "INSTRUMENT_CCY",
       t.ccy_code_settlement "DIVIDEND_CCY",
       t.exchange_rate
FROM   transactions t
       JOIN iv_synonym_lookups iv
         ON t.iv_id = iv.iv_id
WHERE  transaction_type IN ( 'DV', 'FD', 'IN' )
       AND t.id IN (SELECT Max(id)
                    FROM   transactions ta
                    WHERE  transaction_date > SYSDATE -  720 AND trade_date <= SYSDATE
                           AND ta.fund_code in (select fgm.fund_code from fund_group_members fgm where fgm.fg_code = l_TranAccountCode)
                    GROUP  BY applied_transaction_id )
       AND tst_code <> 'XOP'
       AND t.unconverted_gross_amount <> 0;
END SP_IVZ_IFT_EXTRACT_TRANSACTION;

  -- *********************************************************************************************
  -- This Procedure extracts the Account AUM
  -- Parameters:
  --  i_AumAccountCode:  Fund code used to retreive Fund AUM
  -- ************************************************************************************************

PROCEDURE SP_IVZ_IFT_EXTRACT_ACCOUNTAUM(
o_ReturnRecords IN OUT RefCursor,
i_AumAccountCode in VARCHAR2
) AS

l_AumAccountCode varchar2(20);

BEGIN
  l_AumAccountCode := i_AumAccountCode;

OPEN o_ReturnRecords FOR

SELECT POS1.TOTAL_FUND_CODE,
       pos1.ccy_code_base,
       POS1.MARKET_VALUE "INVESTMENT",
       cash_1.MARKET_VALUE + unin_cash.MARKET_VALUE   "CASH",
       --unin_cash.MARKET_VALUE "Uninv_Cash",
       (POS1.MARKET_VALUE + cash_1.MARKET_VALUE + unin_cash.MARKET_VALUE ) "AUM"
  FROM (select pos.total_fund_code,
               F.CCY_CODE_BASE,
               SUM(NVL(pos.mid_market_value,
                       NVL(pos.off_market_value, pos.bid_market_value))) market_value
          from positions           pos,
               iv_synonym_lookups  isl,
               investment_vehicles iv,
               v_iv_class_members  vicm,
               countries           c,
               funds               f
         where pos.iv_id = isl.iv_id
           and pos.iv_id = iv.id
           and vicm.ivcm_iv_id(+) = iv.id
           and iv.cou_code_domiciled_in = c.code
           AND vicm.ivct_code(+) = 'ICB'
           AND pos.total_fund_code = f.code
           and (select to_char(max(position_date), 'DD-MON-YYYY')
                  from positions
                 where total_fund_code = l_AumAccountCode
                   AND ISSUER NOT LIKE '%CASH%') BETWEEN
               vicm.ivcm_effective_start_date AND
               vicm.ivcm_effective_end_date
           and total_fund_code = l_AumAccountCode
           AND POSITION_DATE =
               (select max(position_date)
                  from positions
                 where total_fund_code = l_AumAccountCode
                   AND ISSUER NOT LIKE '%CASH%')
           and pos.time =
               (select max(time)
                  from positions p
                 where total_fund_code = l_AumAccountCode
                   and position_Date =
                       (select max(position_date)
                          from positions
                         where total_fund_code = l_AumAccountCode
                           AND ISSUER NOT LIKE '%CASH%'))
           AND POS.OFFICIAL_YN =
               (select max(official_yn)
                  from positions p
                 where total_fund_code = l_AumAccountCode
                   and position_Date =
                       (select max(position_date)
                          from positions
                         where total_fund_code = l_AumAccountCode
                           AND ISSUER NOT LIKE '%CASH%')
                   and time =
                       (select max(time)
                          from positions
                         where total_fund_code = l_AumAccountCode
                           and position_Date =
                               (select max(position_date)
                                  from positions
                                 where total_fund_code = l_AumAccountCode
                                   AND ISSUER NOT LIKE '%CASH%')))
           and position_Date =
               (select max(position_date)
                  from positions
                 where total_fund_code = l_AumAccountCode
                   AND ISSUER NOT LIKE '%CASH%')
              /*and pos.br_code='ID' AND POS.OFFICIAL_YN='Y'*/
           AND POS.SECID <> 'CASH'
           AND NOT (NVL(pos.price_source, 'NONE') = 'BNYM' and
                pos.accrued_interest <> 0 and pos.mid_position = 0)
           AND NOT ((pos.fmc_code_main LIKE '005%') OR
                ((SUBSTR(pos.fmc_code_main, 1, 3) = '040') OR
                ((SUBSTR(pos.fmc_code_main, 1, 3) >= '010') AND
                (SUBSTR(pos.fmc_code_alternate, 1, 3) < '010'))) OR
                pos.fmc_code_main = '000000000')
         GROUP BY total_fund_code, F.CCY_CODE_BASE) pos1,
       (SELECT CASH.TOTAL_FUND_CODE, CASH.MARKET_VALUE
          FROM (select cc.fund_code "TOTAL_FUND_CODE",
                       (TRADED_CAPITAL_CASH - UNSETTLED_CAPITAL_LONG +
                       MANAGEMENT_FEES_CAPITAL + AIM_STIC +
                       TERM_DEPOSITS_SHORT + SPOT_FX +
                       CLOSED_FORWARD_FX_SHORT - ESTIMATED_DISTRIBUTIONS +
                       COLLATERAL_RECEIVED) +
                       (TRADED_INCOME_CASH - UNSETTLED_INCOME) "MARKET_VALUE"
                  from CASH_COMPONENTS cc
                 where cc.fund_code = l_AumAccountCode
                   AND (select max(position_date)
                          from positions
                         where total_fund_code = l_AumAccountCode
                           AND ISSUER NOT LIKE '%CASH%') = cc.position_date
                   AND (select max(time)
                          from positions p
                         where total_fund_code = l_AumAccountCode
                           and position_Date =
                               (select max(position_date)
                                  from positions
                                 where total_fund_code = l_AumAccountCode AND ISSUER NOT LIKE '%CASH%')
                           ) = cc.position_time) CASH) cash_1,

       (select *
          from (select cc.fund_code "TOTAL_FUND_CODE",
                       (UNSETTLED_CAPITAL_LONG + TRADED_MARGIN_CASH +
                       DEBENTURE_INCLUDE + TOTAL_LOANS_INCLUDE +
                       TERM_DEPOSITS_LONG + REPOS + PENDING_CASH +
                       CONTRA_FUTURES + CLOSED_FORWARD_FX_LONG +
                       OPEN_FORWARD_FX + UNIDENTIFIED_COMPONENT +
                       ACCRUED_INT_SECURITIES + ACCRUED_INT_CASH +
                       COLLATERAL_PAID) + UNSETTLED_INCOME "MARKET_VALUE"
                  from CASH_COMPONENTS cc
                 where cc.fund_code = l_AumAccountCode
                   AND (select max(position_date)
                          from positions
                         where total_fund_code = l_AumAccountCode
                           AND ISSUER NOT LIKE '%CASH%') = cc.position_date
                   AND (select max(time)
                          from positions p
                         where total_fund_code = l_AumAccountCode
                           and position_Date =
                               (select max(position_date)
                                  from positions
                                 where total_fund_code = l_AumAccountCode AND ISSUER NOT LIKE '%CASH%')
                          ) = cc.position_time) un_cash) unin_cash
 where pos1.total_fund_code = cash_1.total_fund_code
   and pos1.total_fund_code = unin_cash.total_fund_code;
END SP_IVZ_IFT_EXTRACT_ACCOUNTAUM;

  -- *********************************************************************************************
  -- SP_IVZ_IFT_EXTRACT_ACCOUNTS Procedure extracts All Active Accounts
  -- Parameters : No Parameter
  -- ************************************************************************************************

PROCEDURE SP_IVZ_IFT_EXTRACT_ACCOUNTS(
o_ReturnRecords IN OUT RefCursor
)AS

BEGIN
OPEN o_ReturnRecords FOR
SELECT F.CODE, F.NAME, LEF.LEI, FS.ISIN
  FROM FUNDS F
  LEFT JOIN LEGAL_ENTITY_FUND_IDS_COLLAT LEF
    ON F.CODE = LEF.PORTFOLIO_CODE
   and PORTFOLIO_CODE_TYPE = 'FMC'
  LEFT JOIN FUND_SHARE_CLASSES FS
    ON F.CODE = FS.FUND_CODE
   and fs.SCT_CODE = 'A'
   AND fs.EFFECTIVE_END_DATE > SYSDATE - 1
 WHERE f.status_code = 'ACTP'
   and fund_sub_fund_indicator = 'F';
END SP_IVZ_IFT_EXTRACT_ACCOUNTS;

END PK_IVZ_IFT_EXTRACT;
