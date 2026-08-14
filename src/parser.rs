use anyhow::Result;
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
// use std::collections::HashMap;
use std::io::BufRead;

use crate::models::Record;

pub fn parse_xml<R: BufRead>(reader: &mut Reader<R>) -> Result<Vec<Record>> {
    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();

    let mut rows: Vec<Record> = Vec::new();

    let mut rec = Record::default();

    let mut last_fare_type = String::new();

    let mut total_cpn_amount: f64 = 0.0;
    let mut temp_cpn_amount: f64 = 0.0;
    let mut temp_tax_amount: f64 = 0.0;
    let mut temp_tax_amount_yr: f64 = 0.0;
    let mut temp_cpn_amount_spam: f64 = 0.0;

    // let mut farecomponent:HashMap<String, String> = HashMap::new();
    // let mut farecouponnumber:String = String::new();

    // STATE FLAGS
    let mut in_coup_standard_comm_amounts_1 = false;
    let mut in_coup_standard_comm_amounts_2 = false;
    let mut in_calculated_amounts = false;
    let mut in_pricing_fares = false;
    let mut wait_for_cpn_lvl = false;
    let mut wait_for_cpn_lvl_yr = false;
    let mut waiting_for_amount_fare = false;
    let mut waiting_for_coup_standard_comm_amount = false;
    let mut waiting_for_std_comm_amount = false;
    let mut waiting_for_supp_comm_amount = false;
    let mut waiting_for_amount_proratedfare = false;
    let mut waiting_for_amount_suppcommissiontype = false;
    let mut wait_for_cpn_lvl_accounted = false;
    let mut waiting_for_amount_fare_roe = false;
    let mut wait_for_rem_lvl_accounted = false;
    let mut wait_for_interline_lvl_accounted = false;
    let mut in_standard_comm_amounts = false;
    let mut in_supp_comm_amounts = false;
    let mut in_prorated_source = false;
    let mut in_suppcommissiontype = false;
    let mut check_first_three_chars = false;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                path.push(tag.clone());

                let path_ref: Vec<&str> = path.iter().map(|s| s.as_str()).collect();

                match path_ref.as_slice() {
                    ["AMA_REV.Feed", "Transaction", "Document"] => {
                        rec.issue_date = get_attr_val(&e, b"DateOfIssuance");
                        rec.validating_carrier = get_attr_val(&e, b"ValidatingCarrier");
                        rec.issue_indicator = get_attr_val(&e, b"IssueIndicator");
                        rec.document_type = get_attr_val(&e, b"Type");
                    }

                    ["AMA_REV.Feed", "Transaction", "Event", "EntityStatus"] => {
                        rec.document_status = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Event", "Event"] => {
                        rec.event_status = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Event", "EventTypeShortCode"] => {
                        rec.event_type_short_code = read_text(reader)?;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "PricingDetails",
                        "CurrencyOfPayment",
                    ] => {
                        rec.currency = read_text(reader)?;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "PricingDetails",
                        "TourCode",
                    ] => {
                        rec.tour_code = read_text(reader)?;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "BookingInformation",
                        "PNRIdentification",
                        "AmadeusRecordLocator",
                        "ID",
                    ] => {
                        rec.pnr_no = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon"] => {
                        let first_three_chars_primary_ticketno = get_attr_val(&e, b"DocumentNbr");
                        if first_three_chars_primary_ticketno.starts_with("232") {
                            check_first_three_chars = true;
                        } else {
                            check_first_three_chars = false;
                        }
                        rec.primary_ticket_no = first_three_chars_primary_ticketno;
                        rec.ticket_no = get_attr_val(&e, b"ConjunctiveDocumentNbr");
                        rec.coupon_no = get_attr_val(&e, b"Number");
                        // let coupon_no = get_attr_val(&e, b"Number");
                        // if let Some(c) = farecomponent.get(&coupon_no) {
                        //     rec.passenger_type_code = c.clone();
                        // } else {
                        //     rec.passenger_type_code = String::new();
                        // }
                        // rec.coupon_no = coupon_no;
                        rec.coupon_status = get_attr_val(&e, b"Status");
                    }

                    // coupon loop row insertion not required
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "SegmentInfo",
                        "CompanyDetails",
                        "MarketingCarrier",
                    ] => {
                        rec.marketting_carrier = read_text(reader)?;
                    }

                    // coupon loop row insertion not required
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "SegmentInfo",
                        "CompanyDetails",
                        "OperatingCarrier",
                    ] => {
                        rec.operating_carrier = read_text(reader)?;
                    }

                    // coupon loop row insertion not required
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CouponDetails",
                        "FareBasisCode",
                    ] => {
                        rec.fare_basis = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Fares", "Fare"] => {
                        in_pricing_fares = true;
                        last_fare_type = get_attr_val(&e, b"FareDescription");
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Fares",
                        "Fare",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_amount_fare = true;
                            waiting_for_amount_fare_roe = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Fares",
                        "Fare",
                        "AccountableEntity",
                        "Amount",
                        "ROE",
                    ] if in_pricing_fares && waiting_for_amount_fare_roe => {
                        rec.exchange_rate = read_text(reader)?;
                        waiting_for_amount_fare_roe = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponStandardCommission",
                    ] => {
                        in_coup_standard_comm_amounts_1 = true;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponStandardCommission",
                        "Commission",
                    ] => {
                        let commision_type = get_attr_val(&e, b"CommissionType");
                        let share_indicator = get_attr_val(&e, b"ShareIndicator");
                        if commision_type == "COAM" && share_indicator == "Y" {
                            in_coup_standard_comm_amounts_2 = true;
                        }
                    }

                    // coupon loop row insertion not required
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponStandardCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] if in_coup_standard_comm_amounts_1 && in_coup_standard_comm_amounts_2 => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_coup_standard_comm_amount = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                    ] => {
                        in_calculated_amounts = true;
                    }

                    // --------------------------
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponSuppCommission",
                        "Commission",
                    ] if in_calculated_amounts => {
                        let suppcommissiontype = get_attr_val(&e, b"CommissionType");
                        if suppcommissiontype == "SPAM" {
                            in_suppcommissiontype = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponSuppCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] if in_suppcommissiontype => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_amount_suppcommissiontype = true;
                        }
                    }
                    // --------------------------

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponProratedFare",
                    ] if in_calculated_amounts => {
                        // let proration_source = get_attr_val(&e, b"ProrationSource");
                        // if proration_source == "1A_SPA" {
                        //     in_prorated_source = true;
                        // }
                        in_prorated_source = true;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponProratedFare",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] if in_prorated_source => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_amount_proratedfare = true;
                        }
                    }

                    // for revenue
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponTaxes",
                        "CollectedTaxesCpnLvl",
                        "Tax",
                    ] => {
                        let nature_code = get_attr_val(&e, b"NatureCode");
                        let iso_code = get_attr_val(&e, b"ISOCode");
                        let is_refundable = get_attr_val(&e, b"IsRefundable");
                        if (nature_code == "AC" || nature_code == "AD") && (iso_code == "YQ") && is_refundable == "N" {
                            wait_for_cpn_lvl = true
                        } else if iso_code == "YR" {
                            wait_for_cpn_lvl_yr = true
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponTaxes",
                        "CollectedTaxesCpnLvl",
                        "Tax",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            wait_for_cpn_lvl_accounted = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponTaxes",
                        "RemittanceTaxesCpnLvl",
                        "RemittanceTaxCpnLvl",
                        "Tax",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            wait_for_rem_lvl_accounted = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponTaxes",
                        "InterlineableTaxes",
                        "Tax",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            wait_for_interline_lvl_accounted = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "StandardCommission",
                        "Commission",
                    ] => {
                        let std_commision_type = get_attr_val(&e, b"CommissionType");
                        if std_commision_type == " " {
                            in_standard_comm_amounts = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "StandardCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] if in_standard_comm_amounts => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_std_comm_amount = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "SupplementaryCommission",
                        "Commission",
                    ] => {
                        let std_commision_type = get_attr_val(&e, b"CommissionType");
                        if std_commision_type == " " {
                            in_supp_comm_amounts = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "SupplementaryCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "AmountType",
                    ] if in_supp_comm_amounts => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_supp_comm_amount = true;
                        }
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "SegmentInfo",
                    ] => {
                        let origin = get_attr_val(&e, b"OriginAirportCode");
                        let dest = get_attr_val(&e, b"DestinationAirportCode");
                        rec.segment = format!("{}{}", origin, dest);
                        rec.dep_date_time = get_attr_val(&e, b"DepartureDate");
                        rec.arr_date_time = get_attr_val(&e, b"ArrivalDate");
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "SegmentInfo",
                        "ClassDetails",
                        "BookingClass",
                    ] => {
                        rec.rbd = read_text(reader)?;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "SegmentInfo",
                        "ClassDetails",
                        "OperatingCabinClass",
                    ] => {
                        rec.cabin = read_text(reader)?;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "SegmentInfo",
                        "FlightIdentification",
                        "OperatingFlightNumber",
                        "FlightNumber",
                    ] => {
                        rec.flight_nr = read_text(reader)?;
                    }

                    _ => {}
                }
            }

            Event::Empty(e) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                path.push(tag.clone());

                let path_ref: Vec<&str> = path.iter().map(|s| s.as_str()).collect();

                match path_ref.as_slice() {
                    ["AMA_REV.Feed", "Transaction", "Document", "IssuanceDetails"] => {
                        rec.pos = get_attr_val(&e, b"CityPOS");
                        rec.iata = get_attr_val(&e, b"Iata");
                        rec.distribution_channel = get_attr_val(&e, b"OfficeId");
                    }

                    //  use this for prod passenger type code when files are unmasked
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "PassengerInformation",
                    ] => {
                        rec.passenger_type_code = get_attr_val(&e, b"PassengerTypeCode");
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Fares",
                        "Fare",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if in_pricing_fares && waiting_for_amount_fare => {
                        let amt = get_attr_val(&e, b"Amount");
                        if last_fare_type == "NET" {
                            rec.net_fare_amount_accounting_currency = amt;
                        } else if last_fare_type == "PUBLISHED" {
                            rec.pub_fare_amount_accounting_currency = amt;
                        } else if last_fare_type == "ADDITIONAL_COLLECTION" {
                            rec.bal_exchange_additional_collected_fare_amount_accounting_currency =
                                amt;
                        }
                        waiting_for_amount_fare = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponProratedFare",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if in_calculated_amounts && waiting_for_amount_proratedfare => {
                        let temp_val = get_attr_val(&e, b"Amount");
                        temp_cpn_amount = temp_val.parse::<f64>().unwrap_or(0.0);
                        rec.cpn_far_fare_amount_accounting_currency = temp_val;
                        waiting_for_amount_proratedfare = false;
                        in_prorated_source = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponSuppCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if in_calculated_amounts && waiting_for_amount_suppcommissiontype => {
                        let temp_val = get_attr_val(&e, b"Amount");
                        temp_cpn_amount_spam = temp_val.parse::<f64>().unwrap_or(0.0);
                        // rec.cpn_far_fare_amount_accounting_currency = temp_val;
                        waiting_for_amount_suppcommissiontype = false;
                        in_suppcommissiontype = false;
                    }

                    // ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponTaxes", "CollectedTaxesCpnLvl", "Tax", "AccountableEntity", "Amount", "Amount"]
                    //     if in_calculated_amounts && wait_for_cpn_lvl_accounted && wait_for_cpn_lvl => {
                    //         let temp_cpnlvl = get_attr_val(&e, b"Amount");
                    //         temp_tax_amount = temp_cpnlvl.parse::<f64>().unwrap_or(0.0);
                    //         rec.cpn_txo_tax_amount_accounting_currency_yq = temp_cpnlvl;

                    //      }
                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponTaxes",
                        "CollectedTaxesCpnLvl",
                        "Tax",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if in_calculated_amounts && wait_for_cpn_lvl_accounted => {
                        let temp_cpnlvl_tax_sum = get_attr_val(&e, b"Amount"); // String

                        let amount: f64 = temp_cpnlvl_tax_sum.parse::<f64>().unwrap_or(0.0);

                        // println!("CPN YQ Amount: {}", amount);
                        total_cpn_amount += amount;

                        if wait_for_cpn_lvl {
                            temp_tax_amount += amount;
                            rec.cpn_txo_tax_amount_accounting_currency_yq = temp_cpnlvl_tax_sum;
                        } else if wait_for_cpn_lvl_yr {
                            temp_tax_amount_yr += amount;
                        }
                        
                        wait_for_cpn_lvl_accounted = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponTaxes",
                        "RemittanceTaxesCpnLvl",
                        "RemittanceTaxCpnLvl",
                        "Tax",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if in_calculated_amounts && wait_for_rem_lvl_accounted => {
                        let temp_remlvl_tax_sum = get_attr_val(&e, b"Amount"); // String

                        let amount: f64 = temp_remlvl_tax_sum.parse::<f64>().unwrap_or(0.0);
                        // println!("REM YQ Amount: {}", amount);

                        total_cpn_amount += amount;
                        wait_for_rem_lvl_accounted = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponTaxes",
                        "InterlineableTaxes",
                        "Tax",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if in_calculated_amounts && wait_for_interline_lvl_accounted => {
                        let temp_interlinelvl_tax_sum = get_attr_val(&e, b"Amount"); // String

                        let amount: f64 = temp_interlinelvl_tax_sum.parse::<f64>().unwrap_or(0.0);
                        // println!("INTERLINE YQ Amount: {}", amount);

                        total_cpn_amount += amount;
                        wait_for_interline_lvl_accounted = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "Coupon",
                        "CalculatedAmounts",
                        "CouponStandardCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if in_coup_standard_comm_amounts_1
                        && in_coup_standard_comm_amounts_2
                        && waiting_for_coup_standard_comm_amount =>
                    {
                        rec.cpn_std_commission_amount_accounting_currency =
                            get_attr_val(&e, b"Amount");
                        waiting_for_coup_standard_comm_amount = false;
                        in_coup_standard_comm_amounts_1 = false;
                        in_coup_standard_comm_amounts_2 = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "StandardCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if waiting_for_std_comm_amount => {
                        rec.std_commission_amount_accounting_currency = get_attr_val(&e, b"Amount");
                        waiting_for_std_comm_amount = false;
                        in_standard_comm_amounts = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "SupplementaryCommission",
                        "Commission",
                        "AccountableEntity",
                        "Amount",
                        "Amount",
                    ] if waiting_for_supp_comm_amount => {
                        rec.sup_commision_amount_accounting_currency = get_attr_val(&e, b"Amount");
                        waiting_for_supp_comm_amount = false;
                        in_supp_comm_amounts = false;
                    }

                    [
                        "AMA_REV.Feed",
                        "Transaction",
                        "Document",
                        "PricingDetails",
                        "RevenueAttributableAgent",
                    ] => {
                        rec.trx_revenue_attributable_iata_number =
                            get_attr_val(&e, b"AgencyNumber");
                    }

                    // [
                    //     "AMA_REV.Feed",
                    //     "Transaction",
                    //     "Document",
                    //     "PricingDetails",
                    //     "FareComponent",
                    //     "RelatedCoupon",
                    // ] => {
                    //     farecouponnumber = get_attr_val(&e, b"CouponNumber");
                    // }

                    // [
                    //     "AMA_REV.Feed",
                    //     "Transaction",
                    //     "Document",
                    //     "PricingDetails",
                    //     "FareComponent",
                    //     "SSPFareInformation",
                    //     "PassengerTypeCode",
                    // ] => {
                    //     let farecouponcode = get_attr_val(&e, b"Code");
                    //     if !farecouponnumber.is_empty() && !farecouponcode.is_empty() {
                    //         farecomponent.insert(farecouponnumber.clone(), farecouponcode.clone());
                    //     }
                    // }

                    _ => {}
                }

                path.pop();
            }

            Event::End(e) => {
                if e.local_name().as_ref() == b"Fares" {
                    in_pricing_fares = false;
                }
                if e.local_name().as_ref() == b"CalculatedAmounts" {
                    in_calculated_amounts = false;
                    // let temp_revenue = temp_cpn_amount + temp_tax_amount;
                    // // rec.sum_cpn_txo_tax_amount_accounting_currency = total_cpn_amount.to_string();
                    // rec.revenue = temp_revenue.to_string();
                    // // total_cpn_amount = 0.0;
                    // temp_cpn_amount = 0.0;
                    // temp_tax_amount = 0.0;
                    // wait_for_cpn_lvl = false;
                    waiting_for_amount_proratedfare = false;
                }
                // if e.local_name().as_ref() == b"CouponStandardCommission" {

                // }

                if e.local_name().as_ref() == b"Tax" {
                    wait_for_cpn_lvl = false;
                    wait_for_cpn_lvl_yr = false;
                }

                // if e.local_name().as_ref() == b"FareComponent" {
                //     farecouponnumber.clear();
                // }

                if e.local_name().as_ref() == b"Coupon" {
                    // push record for completed transaction and reset
                    if check_first_three_chars {
                        let temp_revenue = (temp_cpn_amount + temp_tax_amount + temp_tax_amount_yr) - temp_cpn_amount_spam ;
                        rec.revenue = temp_revenue.to_string();
                    } else {
                        let temp_revenue = temp_cpn_amount;
                        rec.revenue = temp_revenue.to_string();
                    }
                    // let temp_revenue = temp_cpn_amount + temp_tax_amount + temp_tax_amount_yr - temp_cpn_amount_spam ;
                    // rec.sum_cpn_txo_tax_amount_accounting_currency = total_cpn_amount.to_string();
                    rec.prorated_fare_myr = temp_cpn_amount.to_string();
                    rec.coupon_yq_myr = temp_tax_amount.to_string();
                    rec.coupon_yr_myr = temp_tax_amount_yr.to_string();
                    rec.coupon_spam_myr = temp_cpn_amount_spam.to_string();
                    // total_cpn_amount = 0.0;
                    temp_cpn_amount = 0.0;
                    temp_tax_amount = 0.0;
                    temp_tax_amount_yr = 0.0;
                    temp_cpn_amount_spam = 0.0;
                    check_first_three_chars = false;

                    rec.sum_cpn_txo_tax_amount_accounting_currency = total_cpn_amount.to_string();
                    // println!("Pushed record: {:?}", rec);
                    rows.push(rec.clone());
                    total_cpn_amount = 0.0;
                    // wait_for_cpn_lvl_accounted = false;
                    // wait_for_rem_lvl_accounted = false;
                    // wait_for_interline_lvl_accounted = false;
                }

                if e.local_name().as_ref() == b"Transaction" {
                    // push record for completed transaction and reset
                    // rows.push(rec.clone());
                    // farecomponent.clear();
                    rec = Record::default();
                }

                path.pop();
            }

            Event::Eof => break,
            _ => {}
        }

        buf.clear();
    }

    Ok(rows)
}

// To read the text betweent the tags
fn read_text<R: BufRead>(reader: &mut Reader<R>) -> Result<String> {
    let mut buf = Vec::new();
    if let Event::Text(e) = reader.read_event_into(&mut buf)? {
        return Ok(e.unescape()?.to_string());
    }
    Ok(String::new())
}

// To read the attributes within the tags
fn get_attr_val(e: &BytesStart, key: &[u8]) -> String {
    for a in e.attributes().flatten() {
        if a.key.local_name().as_ref() == key {
            return a.unescape_value().unwrap_or_default().to_string();
        }
    }
    String::new()
}
