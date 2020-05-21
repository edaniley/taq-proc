#ifndef TAQ_PREP_INCLUDED
#define TAQ_PREP_INCLUDED

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "taq-proc.h"

namespace taq_prep
{
  enum SecMasterColumn {
    MCOL_Symbol,
    MCOL_Security_Description,
    MCOL_CUSIP,
    MCOL_Security_Type,
    MCOL_SIP_Symbol,
    MCOL_Old_Symbol,
    MCOL_Test_Symbol_Flag,
    MCOL_Listed_Exchange,
    MCOL_Tape,
    MCOL_Unit_Of_Trade,
    MCOL_Round_Lot,
    MCOL_NYSE_Industry_Code,
    MCOL_Shares_Outstanding,
    MCOL_Halt_Delay_Reason,
    MCOL_Specialist_Clearing_Agent,
    MCOL_Specialist_Clearing_Number,
    MCOL_Specialist_Post_Number,
    MCOL_Specialist_Panel,
    MCOL_TradedOnNYSEMKT,
    MCOL_TradedOnNASDAQBX,
    MCOL_TradedOnNSX,
    MCOL_TradedOnFINRA,
    MCOL_TradedOnISE,
    MCOL_TradedOnEdgeA,
    MCOL_TradedOnEdgeX,
    MCOL_TradedOnCHX,
    MCOL_TradedOnNYSE,
    MCOL_TradedOnArca,
    MCOL_TradedOnNasdaq,
    MCOL_TradedOnCBOE,
    MCOL_TradedOnPSX,
    MCOL_TradedOnBATSY,
    MCOL_TradedOnBATS,
    MCOL_TradedOnIEX,
    MCOL_Tick_Pilot_Indicator,
    MCOL_Effective_Date,
    MCOL_TradedOnLTSE,
    MCOL_Max
  };

  enum QuoteColumn {
    QCOL_Time,
    QCOL_Exchange,
    QCOL_Symbol,
    QCOL_Bid_Price,
    QCOL_Bid_Size,
    QCOL_Offer_Price,
    QCOL_Offer_Size,
    QCOL_Quote_Condition,
    QCOL_Sequence_Number,
    QCOL_National_BBO_Ind,
    QCOL_FINRA_BBO_Indicator,
    QCOL_FINRA_ADF_MPID_Indicator,
    QCOL_Quote_Cancel_Correction,
    QCOL_Source_Of_Quote,
    QCOL_Retail_Interest_Indicator,
    QCOL_Short_Sale_Restriction_Indicator,
    QCOL_LULD_BBO_Indicator,
    QCOL_SIP_Generated_Message_Identifier,
    QCOL_National_BBO_LULD_Indicator,
    QCOL_Participant_Timestamp,
    QCOL_FINRA_ADF_Timestamp,
    QCOL_FINRA_ADF_Market_Participant_Quote_Indicator,
    QCOL_Security_Status_Indicator,
    QCOL_Max
  };


  struct AppContext {
    std::string date;
    std::string symb;
    std::string input_type;
    std::vector<std::string> input_files;
    std::string output_dir;
    std::string output_file;
    std::ofstream output;
    Taq::FileHeader output_file_hdr;
    AppContext() : output_file_hdr(1) {}
  };

int ProcessSecMaster(AppContext &, std::istream & is);
int ProcessQuotes(AppContext &, std::istream & is);
int ProcessTrades(AppContext &, std::istream & is);

}

#endif
