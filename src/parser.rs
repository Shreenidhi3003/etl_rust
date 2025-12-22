use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use std::io::BufRead;
use anyhow::Result;

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

    // STATE FLAGS
    let mut in_coup_standard_comm_amounts_1 = false;
    let mut in_coup_standard_comm_amounts_2 = false;
    let mut in_calculated_amounts = false;
    let mut in_pricing_fares = false;
    let mut wait_for_cpn_lvl = false;
    let mut waiting_for_amount_fare = false;
    let mut waiting_for_coup_standard_comm_amount = false;
    let mut waiting_for_std_comm_amount = false;
    let mut waiting_for_supp_comm_amount = false;
    let mut waiting_for_amount_proratedfare = false;
    let mut wait_for_cpn_lvl_accounted = false;
    let mut waiting_for_amount_fare_roe = false;

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
                    }

                    ["AMA_REV.Feed", "Transaction", "Event", "EntityStatus"] => {
                        rec.document_status = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "PricingDetails", "CurrencyOfPayment"] => {
                        rec.currency = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "PricingDetails", "TourCode"] => {
                        rec.tour_code = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "BookingInformation", "PNRIdentification", "AmadeusRecordLocator", "ID"] => {
                        rec.pnr_no = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon"] => {
                        rec.primary_ticket_no = get_attr_val(&e, b"DocumentNbr");
                        rec.ticket_no = get_attr_val(&e, b"ConjunctiveDocumentNbr");
                        rec.coupon_no = get_attr_val(&e, b"Number");
                        rec.coupon_status = get_attr_val(&e, b"Status");
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "SegmentInfo", "CompanyDetails", "MarketingCarrier"] => {
                        rec.marketting_carrier = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "SegmentInfo", "CompanyDetails", "OperatingCarrier"] => {
                        rec.operating_carrier = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CouponDetails", "FareBasisCode"] => {
                        rec.fare_basis = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Fares", "Fare"] => {
                        in_pricing_fares = true;
                        last_fare_type = get_attr_val(&e, b"FareDescription");
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Fares", "Fare", "AccountableEntity", "Amount", "AmountType"] => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_amount_fare = true;
                            waiting_for_amount_fare_roe = true;
                        }
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Fares", "Fare", "AccountableEntity", "Amount", "ROE"] 
                        if in_pricing_fares && waiting_for_amount_fare_roe => {
                            rec.exchange_rate = read_text(reader)?;
                            waiting_for_amount_fare_roe = false;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponStandardCommission"] => {
                        in_coup_standard_comm_amounts_1 = true;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponStandardCommission", "Commission"] => {
                        in_coup_standard_comm_amounts_2 = true;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponStandardCommission", "Commission", "AccountableEntity", "Amount", "AmountType"]
                        if in_coup_standard_comm_amounts_1 && in_coup_standard_comm_amounts_2 =>
                    {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_coup_standard_comm_amount = true;
                        }
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts"] => {
                        in_calculated_amounts = true;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponProratedFare", "AccountableEntity", "Amount", "AmountType"]
                        if in_calculated_amounts =>
                    {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_amount_proratedfare = true;
                        }
                    }

                    // for revenue 
                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponTaxes", "CollectedTaxesCpnLvl", "Tax"] => {
                            let nature_code = get_attr_val(&e, b"NatureCode");
                            let iso_code = get_attr_val(&e, b"ISOCode");
                            let is_refundable =  get_attr_val(&e, b"IsRefundable");
                            if nature_code == "AC" && iso_code == "YQ" && is_refundable == "N" {
                                wait_for_cpn_lvl = true
                            }
                            
                         }
                    
                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponTaxes", "CollectedTaxesCpnLvl", "Tax", "AccountableEntity", "Amount", "AmountType"] 
                        if wait_for_cpn_lvl => {
                            let txt = read_text(reader)?;
                            if txt == "ACCOUNTED" {
                               wait_for_cpn_lvl_accounted = true;
                            }
                            
                         }

                    ["AMA_REV.Feed", "Transaction", "Document", "StandardCommission", "Commission", "AccountableEntity", "Amount", "AmountType"] => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_std_comm_amount = true;
                        }
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "SupplementaryCommission", "Commission", "AccountableEntity", "Amount", "AmountType"] => {
                        let txt = read_text(reader)?;
                        if txt == "ACCOUNTED" {
                            waiting_for_supp_comm_amount = true;
                        }
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "SegmentInfo"] => {
                        let origin = get_attr_val(&e, b"OriginAirportCode");
                        let dest = get_attr_val(&e, b"DestinationAirportCode");
                        rec.segment = format!("{}{}", origin, dest);
                        rec.dep_date_time = get_attr_val(&e, b"DepartureDate");
                        rec.arr_date_time = get_attr_val(&e, b"ArrivalDate");
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "SegmentInfo", "ClassDetails", "BookingClass"] => {
                        rec.rbd = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "SegmentInfo", "ClassDetails", "OperatingCabinClass"] => {
                        rec.cabin = read_text(reader)?;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "SegmentInfo", "FlightIdentification", "OperatingFlightNumber", "FlightNumber"] => {
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

                    ["AMA_REV.Feed", "Transaction", "Document", "Fares", "Fare", "AccountableEntity", "Amount", "Amount"]
                        if in_pricing_fares && waiting_for_amount_fare =>
                    {
                        let amt = get_attr_val(&e, b"Amount");
                        if last_fare_type == "NET" {
                            rec.net_fare_amount_accounting_currency = amt;
                        } else if last_fare_type == "PUBLISHED" {
                            rec.pub_fare_amount_accounting_currency = amt;
                        } else if last_fare_type == "ADDITIONAL_COLLECTION" {
                            rec.bal_exchange_additional_collected_fare_amount_accounting_currency = amt;
                        }
                        waiting_for_amount_fare = false;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponProratedFare", "AccountableEntity", "Amount", "Amount"]
                        if in_calculated_amounts && waiting_for_amount_proratedfare =>
                    {
                        let temp_val = get_attr_val(&e, b"Amount");
                        temp_cpn_amount = temp_val.parse::<f64>().unwrap_or(0.0);
                        rec.cpn_far_fare_amount_accounting_currency = temp_val;
                        waiting_for_amount_proratedfare = false;
                    }

                    // ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponTaxes", "CollectedTaxesCpnLvl", "Tax", "AccountableEntity", "Amount", "Amount"] 
                    //     if in_calculated_amounts && wait_for_cpn_lvl_accounted && wait_for_cpn_lvl => {
                    //         let temp_cpnlvl = get_attr_val(&e, b"Amount");
                    //         temp_tax_amount = temp_cpnlvl.parse::<f64>().unwrap_or(0.0);
                    //         rec.cpn_txo_tax_amount_accounting_currency_yq = temp_cpnlvl;
                            
                    //      }
                    
                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponTaxes", "CollectedTaxesCpnLvl", "Tax", "AccountableEntity", "Amount", "Amount"] 
                        if in_calculated_amounts && wait_for_cpn_lvl_accounted => {
                            let temp_cpnlvl_tax_sum = get_attr_val(&e, b"Amount"); // String

                            let amount: f64 = temp_cpnlvl_tax_sum
                                .parse::<f64>()
                                .unwrap_or(0.0);

                            total_cpn_amount += amount;

                            if wait_for_cpn_lvl {
                                temp_tax_amount = amount;
                                rec.cpn_txo_tax_amount_accounting_currency_yq = temp_cpnlvl_tax_sum;

                            }
                         }

                    ["AMA_REV.Feed", "Transaction", "Document", "Coupon", "CalculatedAmounts", "CouponStandardCommission", "Commission", "AccountableEntity", "Amount", "Amount"]
                        if in_coup_standard_comm_amounts_1 && in_coup_standard_comm_amounts_2 && waiting_for_coup_standard_comm_amount =>
                    {
                        rec.cpn_std_commission_amount_accounting_currency = get_attr_val(&e, b"Amount");
                        waiting_for_coup_standard_comm_amount = false;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "StandardCommission", "Commission", "AccountableEntity", "Amount", "Amount"]
                        if waiting_for_std_comm_amount =>
                    {
                        rec.std_commission_amount_accounting_currency = get_attr_val(&e, b"Amount");
                        waiting_for_std_comm_amount = false;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "SupplementaryCommission", "Commission", "AccountableEntity", "Amount", "Amount"]
                        if waiting_for_supp_comm_amount =>
                    {
                        rec.sup_commision_amount_accounting_currency = get_attr_val(&e, b"Amount");
                        waiting_for_supp_comm_amount = false;
                    }

                    ["AMA_REV.Feed", "Transaction", "Document", "PricingDetails", "RevenueAttributableAgent"] => {
                        rec.trx_revenue_attributable_iata_number = get_attr_val(&e, b"AgencyNumber");
                    }

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
                    let temp_revenue = temp_cpn_amount + temp_tax_amount;
                    rec.sum_cpn_txo_tax_amount_accounting_currency = total_cpn_amount.to_string();
                    rec.revenue = temp_revenue.to_string();
                    temp_cpn_amount = 0.0;
                    temp_tax_amount = 0.0;
                    wait_for_cpn_lvl_accounted = false;
                    wait_for_cpn_lvl = false;
                    waiting_for_amount_proratedfare = false;
                }
                if e.local_name().as_ref() == b"CouponStandardCommission" {
                    in_coup_standard_comm_amounts_1 = false;
                    in_coup_standard_comm_amounts_2 = false;
                }

                if e.local_name().as_ref() == b"Transaction" {
                    // push record for completed transaction and reset
                    rows.push(rec);
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
