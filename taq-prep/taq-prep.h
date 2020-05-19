#ifndef TAQ_PREP_INCLUDED
#define TAQ_PREP_INCLUDED

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "taq-proc.h"

namespace taq_prep
{
  enum QuoteColum {
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
    std::ofstream output;
  };

int ProcessSecMaster(AppContext &, std::istream & is);
int ProcessQuotes(AppContext &, std::istream & is);
int ProcessTrades(AppContext &, std::istream & is);

}

#endif
