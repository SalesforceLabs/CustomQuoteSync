trigger QuoteLineSyncTrigger on QuoteLineItem (before insert, after insert, after update) {
 
    if (trigger.isBefore && trigger.isInsert) { 
        if (QuoteSyncUtil.isRunningTest) {
            for (QuoteLineItem qli : trigger.new) {
                QuoteSyncUtil.populateRequiredFields(qli);
            }
        }    
        return;
    } 
  
    if (TriggerStopper.stopQuoteLine) return;
        
    Set<String> quoteLineFields = QuoteSyncUtil.getQuoteLineFields();
    List<String> oppLineFields = QuoteSyncUtil.getOppLineFields();
    
    String qliFields = QuoteSyncUtil.getQuoteLineFieldsString();
    
    String oliFields = QuoteSyncUtil.getOppLineFieldsString();
            
    String qliIds = '';
    for (QuoteLineItem qli : trigger.new) {
        if (qliIds != '') qliIds += ', ';
        qliIds += '\'' + qli.Id + '\'';
    }
     
    String qliQuery = 'select Id, QuoteId, PricebookEntryId, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + qliFields + ' from QuoteLineItem where Id in (' + qliIds + ') order by QuoteId, SortOrder ASC';
    //System.debug(qliQuery); 
        
    List<QuoteLineItem> qlis = Database.query(qliQuery);
    
    Map<Id, List<QuoteLineItem>> quoteToQliMap = new Map<Id, List<QuoteLineItem>>();
    
    for (QuoteLineItem qli : qlis) {    
        List<QuoteLineItem> qliList = quoteToQliMap.get(qli.QuoteId);
        if (qliList == null) {
            qliList = new List<QuoteLineItem>();
        } 
        qliList.add(qli);  
        quoteToQliMap.put(qli.QuoteId, qliList);        
    }

    Set<Id> quoteIds = quoteToQliMap.keySet();
    Map<Id, Quote> quotes = new Map<Id, Quote>([select id, OpportunityId, isSyncing from Quote where Id in :quoteIds]);
    
    String oppIds = '';
    Set<Id> filterQuoteIds = new Set<Id>();
    for (Quote quote : quotes.values()) {
        // Only sync quote line item that are inserted for a new Quote or on a isSyncing Quote
        if ((trigger.isInsert && QuoteSyncUtil.isNewQuote(quote.Id)) || quote.isSyncing) {
           if (oppIds != '') oppIds += ', ';
           oppIds += '\'' + quote.OpportunityId + '\'';         
        } else {
            filterQuoteIds.add(quote.Id);
        }
    }
    
    //System.debug('Filter quote ids: ' + filterQuoteIds);
    
    quoteIds.removeAll(filterQuoteIds);
    for (Id id : filterQuoteIds) {
       quotes.remove(id);
       quoteToQliMap.remove(id);
    }   
   
    if (oppIds != '') {   
        String oliQuery = 'select Id, OpportunityId, PricebookEntryId, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + oliFields + ' from OpportunityLineItem where OpportunityId in (' + oppIds + ') order by OpportunityId, SortOrder ASC';   
        //System.debug(qliQuery);    
        
        List<OpportunityLineItem> olis = Database.query(oliQuery);    
        
        Map<Id, List<OpportunityLineItem>> oppToOliMap = new Map<Id, List<OpportunityLineItem>>();
        
        for (OpportunityLineItem oli : olis) {
            List<OpportunityLineItem> oliList = oppToOliMap.get(oli.OpportunityId);
            if (oliList == null) {
                oliList = new List<OpportunityLineItem>();
            } 
            oliList.add(oli);  
            oppToOliMap.put(oli.OpportunityId, oliList);       
        } 
     
        Set<OpportunityLineItem> updateOlis = new Set<OpportunityLineItem>();
        Set<QuoteLineItem> updateQlis = new Set<QuoteLineItem>();
              
        for (Quote quote : quotes.values()) {
                
            List<OpportunityLineItem> opplines = oppToOliMap.get(quote.OpportunityId);
            
            // for quote line insert, there will not be corresponding opp line
            if (opplines == null) continue;        

            Set<OpportunityLineItem> matchedOlis = new Set<OpportunityLineItem>();
        
            for (QuoteLineItem qli : quoteToQliMap.get(quote.Id)) {
            
                boolean updateOli = false;
                QuoteLineItem oldQli = null;
                
                if (trigger.isUpdate) {
                    oldQli = trigger.oldMap.get(qli.Id);
                    //System.debug('Old qli: ' + oldQli.UnitPrice + ', ' + oldQli.Quantity + ', ' + oldQli.Discount + ', ' + oldQli.ServiceDate);
                    //System.debug('New qli: ' + qli.UnitPrice + ', ' + qli.Quantity + ', ' + qli.Discount + ', ' + qli.ServiceDate);
                    
                    if (qli.UnitPrice == oldQli.UnitPrice
                        && qli.Quantity == oldQli.Quantity
                        && qli.Discount == oldQli.Discount
                        && qli.ServiceDate == oldQli.ServiceDate
                        && qli.SortOrder == oldQli.SortOrder 
                       )
                        updateOli = true;                       
                }
                
                                                                      
                boolean hasChange = false;
                boolean match = false;
                  
                for (OpportunityLineItem oli : opplines) {          
                    if (oli.pricebookentryid == qli.pricebookentryId  
                        && oli.UnitPrice == qli.UnitPrice
                        && oli.Quantity == qli.Quantity
                        && oli.Discount == qli.Discount
                        && oli.ServiceDate == qli.ServiceDate
                        && oli.SortOrder == qli.SortOrder
                       ) {
                        
                        if (updateOlis.contains(oli) || matchedOlis.contains(oli)) continue;  
                        
                        matchedOlis.add(oli);                       
                                               
                        for (String qliField : quoteLineFields) {
                            String oliField = QuoteSyncUtil.getQuoteLineFieldMapTo(qliField);
                            Object oliValue = oli.get(oliField);
                            Object qliValue = qli.get(qliField);
                            if (oliValue != qliValue) { 
                                                                                                
                                if (trigger.isInsert && (qliValue == null || (qliValue instanceof Boolean && !Boolean.valueOf(qliValue)))) {
                                
                                    //System.debug('Insert trigger, isSyncing: ' + quote.isSyncing + ', new quote ids: ' + QuoteSyncUtil.getNewQuoteIds());
                                    
                                    // If it's a newly created Quote, don't sync the "Description" field value, 
                                    // because it's already copied from Opportunity Line Item on create. 
                                    if (quote.isSyncing || (QuoteSyncUtil.isNewQuote(quote.Id) && !qliField.equalsIgnoreCase('description'))) {                                     
                                        qli.put(qliField, oliValue);
                                        hasChange = true; 
                                    }    
                                   
                                } else if (trigger.isUpdate && !updateOli /*&& oldQli != null*/) {
                                    //Object oldQliValue = oldQli.get(qliField);
                                    //if (qliValue == oldQliValue) {
                                        if (oliValue == null) qli.put(qliField, null);
                                        else qli.put(qliField, oliValue);
                                        hasChange = true;
                                    //}     
                                     
                                } else if (trigger.isUpdate && updateOli) {
                                    if (qliValue == null) oli.put(oliField, null);
                                    else oli.put(oliField, qliValue);
                                    hasChange = true;  
                                }
                            }    
                        }
                        
                        if (hasChange) {
                            if (trigger.isInsert || (trigger.isUpdate && !updateOli)) { 
                                updateQlis.add(qli);
                            } else if (trigger.isUpdate && updateOli) {                               
                                updateOlis.add(oli);
                            }                    
                        } 
                        
                        match = true;      
                        break;                          
                    } 
                }
                
                // NOTE: this cause error when there is workflow field update that fired during record create
                //if (trigger.isUpdate && updateOli) System.assert(match, 'No matching oppline');     
            }
        }
     
        TriggerStopper.stopOpp = true;
        TriggerStopper.stopQuote = true;             
        TriggerStopper.stopOppLine = true;
        TriggerStopper.stopQuoteLine = true;    
                    
        if (!updateOlis.isEmpty()) { 
            List<OpportunityLineItem> oliList = new List<OpportunityLineItem>();
            oliList.addAll(updateOlis);
                            
            Database.update(olilist);              
        }
        
        if (!updateQlis.isEmpty()) {
            List<QuoteLineItem> qliList = new List<QuoteLineItem>();   
            qliList.addAll(updateQlis);
                              
            Database.update(qliList);            
        }
        
        if (Trigger.isInsert) {
           QuoteSyncUtil.removeAllNewQuoteIds(quoteIds);
        }                             
        
        TriggerStopper.stopOpp = false;
        TriggerStopper.stopQuote = false;                
        TriggerStopper.stopOppLine = false;          
        TriggerStopper.stopQuoteLine = false;           
    }    
}