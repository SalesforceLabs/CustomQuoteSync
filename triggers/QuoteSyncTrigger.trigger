trigger QuoteSyncTrigger on Quote (after insert, after update) {
    if (TriggerStopper.stopQuote) return;
        
    TriggerStopper.stopQuote = true;    

    Set<String> quoteFields = QuoteSyncUtil.getQuoteFields();
    List<String> oppFields = QuoteSyncUtil.getOppFields();
    
    String quote_fields = QuoteSyncUtil.getQuoteFieldsString();
    
    String opp_fields = QuoteSyncUtil.getOppFieldsString();

    Map<Id, Id> startSyncQuoteMap = new Map<Id, Id>();
    String quoteIds = '';
    for (Quote quote : trigger.new) {
        if (quote.isSyncing && !trigger.oldMap.get(quote.Id).isSyncing) {
            startSyncQuoteMap.put(quote.Id, quote.OpportunityId);
        }
        
        if (quoteIds != '') quoteIds += ', ';
        quoteIds += '\'' + quote.Id + '\'';
    }

    String quoteQuery = 'select Id, OpportunityId, isSyncing' + quote_fields + ' from Quote where Id in (' + quoteIds + ')';
    //System.debug(quoteQuery);     

    List<Quote> quotes = Database.query(quoteQuery);
    
    String oppIds = '';    
    Map<Id, Quote> quoteMap = new Map<Id, Quote>();
    
    for (Quote quote : quotes) {
        if (trigger.isInsert || (trigger.isUpdate && quote.isSyncing)) {
            quoteMap.put(quote.OpportunityId, quote);
            if (oppIds != '') oppIds += ', ';
            oppIds += '\'' + quote.opportunityId + '\'';            
        }
    }
    
    if (oppIds != '') {
        String oppQuery = 'select Id, HasOpportunityLineItem' + opp_fields + ' from Opportunity where Id in (' + oppIds + ')';
        //System.debug(oppQuery);     
    
        List<Opportunity> opps = Database.query(oppQuery);
        List<Opportunity> updateOpps = new List<Opportunity>();
        List<Quote> updateQuotes = new List<Quote>();        
        
        for (Opportunity opp : opps) {
            Quote quote = quoteMap.get(opp.Id);
            
            // store the new quote Id if corresponding opportunity has line items
            if (trigger.isInsert && opp.HasOpportunityLineItem) {
                QuoteSyncUtil.addNewQuoteId(quote.Id);
            }
            
            boolean hasChange = false;
            for (String quoteField : quoteFields) {
                String oppField = QuoteSyncUtil.getQuoteFieldMapTo(quoteField);
                Object oppValue = opp.get(oppField);
                Object quoteValue = quote.get(quoteField);
                if (oppValue != quoteValue) {                   
                    if (trigger.isInsert && (quoteValue == null || (quoteValue instanceof Boolean && !Boolean.valueOf(quoteValue)))) {
                        quote.put(quoteField, oppValue);
                        hasChange = true;                          
                    } else if (trigger.isUpdate) {
                        if (quoteValue == null) opp.put(oppField, null);
                        else opp.put(oppField, quoteValue);
                        hasChange = true;                          
                    }                    
                }                     
            }    
            if (hasChange) {
                if (trigger.isInsert) { 
                    updateQuotes.add(quote);
                } else if (trigger.isUpdate) {
                    updateOpps.add(opp);                
                }               
            }                                  
        } 
   
        if (trigger.isInsert) {
            Database.update(updateQuotes);
        } else if (trigger.isUpdate) {
            TriggerStopper.stopOpp = true;            
            Database.update(updateOpps);
            TriggerStopper.stopOpp = false;              
        }    
    }
       
    TriggerStopper.stopQuote = false; 
}