trigger OppSyncTrigger on Opportunity (after update) {
    if (TriggerStopper.stopOpp) return;
        
    TriggerStopper.stopOpp = true;    

    Set<String> quoteFields = QuoteSyncUtil.getQuoteFields();
    List<String> oppFields = QuoteSyncUtil.getOppFields();
    
    String quote_fields = QuoteSyncUtil.getQuoteFieldsString();
    
    String opp_fields = QuoteSyncUtil.getOppFieldsString();    
    
    Map<Id, Id> startSyncQuoteMap = new Map<Id, Id>();    
    String oppIds = '';
    for (Opportunity opp : trigger.new) {
        if (opp.SyncedQuoteId != trigger.oldMap.get(opp.Id).SyncedQuoteId) {
            if (opp.SyncedQuoteId != null) {
                startSyncQuoteMap.put(opp.SyncedQuoteId, opp.Id);
            }                
            continue;
        }    
        if (oppIds != '') oppIds += ', ';
        oppIds += '\'' + opp.Id + '\'';
    }

    if (oppIds != '') {
        String oppQuery = 'select Id, SyncedQuoteId' + opp_fields + ' from Opportunity where Id in (' + oppIds + ') and SyncedQuoteId != null';
        //System.debug(oppQuery);     
    
        List<Opportunity> opps = Database.query(oppQuery);
        
        String quoteIds = '';
        Map<Id, Opportunity> oppMap = new Map<Id, Opportunity>();
                    
        for (Opportunity opp : opps) {
            if (opp.SyncedQuoteId != null) {
                oppMap.put(opp.Id, opp);
                if (quoteIds != '') quoteIds += ', ';
                quoteIds += '\'' + opp.SyncedQuoteId + '\'';            
            }
        }
        
        if (quoteIds != '') { 
            String quoteQuery = 'select Id, OpportunityId' + quote_fields + ' from Quote where Id in (' + quoteIds + ')';
            //System.debug(quoteQuery);
            
            List<Quote> quotes = Database.query(quoteQuery);
            List<Quote> updateQuotes = new List<Quote>();
            
            for (Quote quote : quotes) {
                Opportunity opp = oppMap.get(quote.OpportunityId);
                boolean hasChange = false;
                for (String quoteField : quoteFields) {
                    String oppField = QuoteSyncUtil.getQuoteFieldMapTo(quoteField);
                    Object oppValue = opp.get(oppField);
                    Object quoteValue = quote.get(quoteField);
                    if (oppValue != quoteValue) {
                        hasChange = true;
                        if (oppValue == null) quote.put(quoteField, null);
                        else quote.put(quoteField, oppValue);                                             
                    }                     
                }
                if (hasChange) updateQuotes.add(quote);                                  
            } 
            TriggerStopper.stopQuote = true;           
            Database.update(updateQuotes);
            TriggerStopper.stopQuote = false;             
        }
    }
    
    // Check start sync quote with matching opp lines and quote lines
    
    if (!startSyncQuoteMap.isEmpty()) {
    
        String syncQuoteIds = '';
        String syncOppIds = '';
        
        for (Id quoteId : startSyncQuoteMap.keySet()) {
            if (syncQuoteIds != '') syncQuoteIds += ', ';
            syncQuoteIds += '\'' + quoteId + '\'';
                           
            if (syncOppIds != '') syncOppIds += ', ';
            syncOppIds += '\'' + startSyncQuoteMap.get(quoteId) + '\'';
        }
        
          
        String qliFields = QuoteSyncUtil.getQuoteLineFieldsString();    
        String oliFields = QuoteSyncUtil.getOppLineFieldsString(); 
                
        String qliQuery = 'select Id, QuoteId, PricebookEntryId, UnitPrice, Quantity, Discount, SortOrder' + qliFields + ' from QuoteLineItem where QuoteId in (' + syncQuoteIds + ') order by QuoteId, SortOrder ASC';           
        String oliQuery = 'select Id, OpportunityId, PricebookEntryId, UnitPrice, Quantity, Discount, SortOrder' + oliFields + ' from OpportunityLineItem where OpportunityId in (' + syncOppIds + ') order by OpportunityId, SortOrder ASC';   
    
        List<QuoteLineItem> qlis = Database.query(qliQuery);   
        List<OpportunityLineItem> olis = Database.query(oliQuery);
        
        Map<Id, List<OpportunityLineItem>> oppToOliMap = new Map<Id, List<OpportunityLineItem>>();
        Map<Id, List<QuoteLineItem>> quoteToQliMap = new Map<Id, List<QuoteLineItem>>();        
        
        for (QuoteLineItem qli : qlis) {
            List<QuoteLineItem> qliList = quoteToQliMap.get(qli.QuoteId);
            if (qliList == null) {
                qliList = new List<QuoteLineItem>();
            } 
            qliList.add(qli);  
            quoteToQliMap.put(qli.QuoteId, qliList);        
        }
        
        for (OpportunityLineItem oli : olis) {
            List<OpportunityLineItem> oliList = oppToOliMap.get(oli.OpportunityId);
            if (oliList == null) {
                oliList = new List<OpportunityLineItem>();
            } 
            oliList.add(oli);  
            oppToOliMap.put(oli.OpportunityId, oliList);       
        }        
          
        Set<OpportunityLineItem> updateOliSet = new Set<OpportunityLineItem>(); 
        List<OpportunityLineItem> updateOliList = new List<OpportunityLineItem>(); 
        Set<String> quoteLineFields = QuoteSyncUtil.getQuoteLineFields();
          
        for (Id quoteId : startSyncQuoteMap.keySet()) {
            Id oppId = startSyncQuoteMap.get(quoteId);
            List<QuoteLineItem> quotelines = quoteToQliMap.get(quoteId);
            List<OpportunityLineItem> opplines = oppToOliMap.get(oppId);
            
            if (quotelines != null && opplines != null && !quotelines.isEmpty() && !opplines.isEmpty()) {
            
                for (QuoteLineItem qli : quotelines) {
                    boolean hasChange = false;
                                                  
                    for (OpportunityLineItem oli : opplines) {
                        if (oli.pricebookentryid == qli.pricebookentryId  
                            && oli.UnitPrice == qli.UnitPrice
                            && oli.Quantity == qli.Quantity
                            && oli.Discount == qli.Discount
                            && oli.SortOrder == qli.SortOrder
                           ) {
                           
                            if (updateOliSet.contains(oli)) continue; 
                            
                            //System.debug('########## qliId: ' + qli.Id + '     oliId: ' + oli.Id);
                              
                            for (String qliField : quoteLineFields) {
                                String oliField = QuoteSyncUtil.getQuoteLineFieldMapTo(qliField);
                                Object oliValue = oli.get(oliField);
                                Object qliValue = qli.get(qliField);
                                if (oliValue != qliValue) {
                                    hasChange = true;
                                    if (qliValue == null) oli.put(oliField, null);
                                    else oli.put(oliField, qliValue);                                                                
                                }    
                            }
                            
                            if (hasChange) {
                                updateOliSet.add(oli);
                            }
                                
                            break;        
                        }                        
                    }
                }
            }
         }
         
         if (!updateOliSet.isEmpty()) {
             updateOliList.addAll(updateOliSet);
             
             TriggerStopper.stopQuote = true;             
             TriggerStopper.stopOppLine = true;
             TriggerStopper.stopQuoteLine = true;  
                        
             Database.update(updateOliList);
             updateOliSet.clear();
             updateOliList.clear();
             
             TriggerStopper.stopQuote = false;             
             TriggerStopper.stopOppLine = false;
             TriggerStopper.stopQuoteLine = false;                               
         }                
    }
            
    TriggerStopper.stopOpp = false; 
}