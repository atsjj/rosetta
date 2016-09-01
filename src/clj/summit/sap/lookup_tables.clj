(ns summit.sap.lookup-tables)

(def order-type-descripts
  {"ZCO" "Counter Sales Order",
   "ZCR" "Credit Memo Request",
   "ZDR" "Debit Memo Request",
   "ZFDS" "Factory Direct Ship",
   "ZIO" "Internet Order",
   "ZKA" "Consignment Pick-up",
   "ZKB" "Consignment Fill-up",
   "ZKE" "Consignment Issue",
   "ZKR" "Consignment Returns",
   "ZLDS" "Lot Order Direct Ship",
   "ZLTS" "Lot Order Thru Stock",
   "ZOR" "Standard Order",
   "ZRE" "Returns"
   })

(def doc-ranges
  [
   [(range 0000000001  500000000), :order],
   [(range 2000000000 2500000000), :quote],
   [(range 4000000000 4500000000), :contract],
   [(range 6000000000 6500000000), :return],
   [(range 7000000000 7500000000), :debit_memo],
   [(range 8000000000 8400000000), :delivery],
   [(range 8400000000 8500000000), :return_delivery],
   [(range 9000000000 9500000000), :invoice],
   ])
;; (count (first doc-ranges))

(def vb-types
  {"A" "Inquiry",
   "B" "Quotation",
   "C" "Order",
   "D" "Item proposal",
   "E" "Scheduling agreement",
   "F" "Scheduling agreement with external service agent",
   "G" "Contract",
   "H" "Returns",
   "I" "Order w/o Charge",
   "J" "Delivery",
   "M" "Invoice",
   "O" "Credit Memo",   ; return invoice
   "P" "Debit Memo",
   "T" "Return Delivery",
   })

(def shipping-types
  {"Z0" "Will-Call",
   "Z1" "Summit Truck",
   "Z2" "Direct-Ship",
   "Z3" "Salesman",
   "Z4" "Forwarding Agent",
   "Z5" "UPS",
   "Z6" "FED EX",
   ""   ""
   })

(def papi-shipping-types
  {"will-call" "Z0",
   "summit-truck" "Z1",
   "ground" "Z5",
   "2nd-day" "Z5",
   "overnight" "Z5",
   })

(def papi-sub-shipping-types
  {"ground" "UPSG",
   "2nd-day" "UPSB",
   "overnight" "UPSR",
   })

(def deliver-statuses
  {"A" "Pending",
   "B" "Partially Delivered",
   "C" "Completed",
   " " "Not Relevant",
   ""  "Not Relevant",
   })

(def payment-statuses
  {"X" "Open",
   "x" "Open",
   " " "Cleared",
   ""  "Cleared",
   })

(def invoice-types
  {"RE" :return_invoice,
   "L2" :debit_memo,
   })

(def open-item-types
  {"AB" "internal-offset",
   "DA" "customer-document",
   "DG" "tax-adjustment",
   "DR" "manual-debit",
   "DZ" "incoming-payment",
   "RV" "invoice",
   "TC" "tax-credit",
   "TD" "tax-debit",
   "Z2" "counter-sale-closing",
   "Z4" "credit-card-transaction",
   "ZK" "service-charge"
   })

  ;; def delivery_status_descript(from_status)
  ;;   DELIVERY_STATUSES.fetch(from_status) { from_status }
  ;; end

  ;; def payment_status_descript(from_status)
  ;;   PAYMENT_STATUSES.fetch(from_status.to_s.strip) { from_status }
  ;; end

  ;; def shipping_type_descript(shipping_type)
  ;;   SHIPPING_TYPES.fetch(shipping_type) { 'Unknown' }
  ;; end

  ;; def invoice_type(invoice_typ)
  ;;   INVOICE_TYPES.fetch(invoice_typ.to_s) { :order_invoice }
  ;; end

  ;; def order_type_descript(abbv)
  ;;   ORDER_TYPE_DESCRIPTS.fetch(abbv) { 'Unknown' }
  ;; end

  ;; def document_type(doc_num)
  ;;   return nil if doc_num.class==String and (doc_num=~/[0-9]+$/) != 0
  ;;   num = doc_num.to_i
  ;;   DOC_RANGES.each { |range| return range[1] if range[0].include? num }
  ;;   return nil
  ;; end

  ;; def open_item_type(type)
  ;;   OPEN_ITEM_TYPES.fetch(type) { 'unknown' }
  ;; end
