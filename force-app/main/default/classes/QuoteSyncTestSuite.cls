@isTest 
private class QuoteSyncTestSuite {
    
    private static Account acc;
    private static Pricebook2 pb;
    private static Product2 prod;
    private static PricebookEntry pbe;
    
    static {    
        pb = [select Id, isActive from Pricebook2 where isStandard=true];
        if (!pb.isActive) {
             pb.isActive = true;
             update(pb);
        }     
                   
        prod = new Product2(Name='test product', IsActive=true);             
        insert(prod);                            

        pbe = new PricebookEntry(Pricebook2Id=pb.Id, Product2Id=prod.Id, IsActive=true, UnitPrice=10);
        insert(pbe);                   
    
        acc = new Account(name='test acc');         
        insert(acc);                                 
    }

    private static void assertQuoteSync(String oppId, String quoteId) {
        assertQuoteSync(oppId, quoteId, true, false);
    }

    private static void assertQuoteNotSync(String oppId, String quoteId, boolean isCreate) {
        assertQuoteSync(oppId, quoteId, false, isCreate);
    }

    private static void assertQuoteNotSync(String oppId, String quoteId) {
        assertQuoteSync(oppId, quoteId, false, false);    
    }
    
    private static void assertQuoteSync(String oppId, String quoteId, boolean sync, boolean isCreate) {
    
        String quote_fields = QuoteSyncUtil.getQuoteFieldsString();
        String opp_fields = QuoteSyncUtil.getOppFieldsString();
        
        String oppQuery = 'select Id, SyncedQuoteId' + opp_fields + ' from Opportunity where Id = \'' + oppId + '\'';
        String quoteQuery = 'select Id, OpportunityId' + quote_fields + ' from Quote where Id = \'' + quoteId + '\'';
        
        Opportunity opp = Database.query(oppQuery);
        Quote quote = Database.query(quoteQuery);
        
        for (String quoteField : QuoteSyncUtil.getQuoteFields()) {
            String oppField = QuoteSyncUtil.getQuoteFieldMapTo(quoteField);
            Object oppValue = opp.get(oppField);
            Object quoteValue = quote.get(quoteField); 
            
            // boolean field is equal even not sync because of trigger on Quote creation copy values from Opp 
            if (sync || (oppValue instanceof Boolean && quoteValue instanceof Boolean && isCreate)) {
                System.assertEquals(oppValue, quoteValue, 'Opportunity.' + oppField + ' value does not match Quote.' + quoteField + ' value'); 
            } else {
                System.assertNotEquals(oppValue, quoteValue, 'Opportunity.' + oppField + ' value should not match Quote.' + quoteField + ' value');             
            }    
        }                                       
    }
  
    private static void assertQuoteLineSync(String oppId, String quoteId) {
        assertQuoteLineSync(oppId, quoteId, true, false);
    }
 
    private static void assertQuoteLineNotSync(String oppId, String quoteId) {
        assertQuoteLineSync(oppId, quoteId, false, false);
    } 
    
    private static void assertQuoteLineNotSync(String oppId, String quoteId, boolean isCreate) {
        assertQuoteLineSync(oppId, quoteId, false, isCreate);
    }       
      
  
    private static void assertQuoteLineSync(String oppId, String quoteId, boolean sync, boolean isCreate) {
    
        String qliFields = QuoteSyncUtil.getQuoteLineFieldsString();    
        String oliFields =  QuoteSyncUtil.getOppLineFieldsString();
        
        String qliQuery = 'select Id, QuoteId, PricebookEntryId, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + qliFields + ' from QuoteLineItem where QuoteId = \'' + quoteId + '\'';
        String oliQuery = 'select Id, OpportunityId, PricebookEntryId, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + oliFields + ' from OpportunityLineItem where OpportunityId  = \'' + oppId + '\'';        

        List<OpportunityLineItem> olis = Database.query(oliQuery);
        List<QuoteLineItem> qlis = Database.query(qliQuery);
        
        if (sync) {
        
            String errorMsg = '';
            if (olis.size() != qlis.size()) {
            
                Integer n = 0;
                for (OpportunityLineItem oli : olis) {
                    n++;
                    String oliValues = '#################   OppLine ' + n + ': ' + oli.PricebookEntryId + ', ' + oli.UnitPrice + ', ' + oli.Quantity + ', ' + oli.Discount + ', ' + oli.ServiceDate;
                    
                    for (String oliField : QuoteSyncUtil.getOppLineFields()) {
                        oliValues += ', ' + oli.get(oliField);
                    }
                    
                    System.debug(oliValues);
                    errorMsg += oliValues + '\n';
                }
                
                n = 0;
                for (QuoteLineItem qli : qlis) {
                    n++;
                    String qliValues = '#################   QuoteLine ' + n + ': ' + qli.PricebookEntryId + ', ' + qli.UnitPrice + ', ' + qli.Quantity + ', ' + qli.Discount + ', ' + qli.ServiceDate;
                    
                    for (String qliField : QuoteSyncUtil.getQuoteLineFields()) {
                        qliValues += ', ' + qli.get(qliField);
                    }
                                        
                    System.debug(qliValues);  
                    errorMsg += qliValues + '\n';              
                }
            }
                
            System.assertEquals(olis.size(), qlis.size(), 'Number of opp lines: ' + olis.size() + ' does not match number of quote lines: ' + qlis.size() + '\n' + errorMsg);
        
        }
        
        for (QuoteLineItem qli : qlis) { 
            boolean match = false;
            for (OpportunityLineItem oli : olis) {          
                if (oli.PricebookEntryId == qli.PricebookEntryId && oli.UnitPrice == qli.UnitPrice 
                    && oli.Quantity == qli.Quantity && oli.Discount == qli.Discount 
                    && oli.ServiceDate == qli.ServiceDate && oli.SortOrder == qli.SortOrder) { 
       
                    for (String qliField : QuoteSyncUtil.getQuoteLineFields()) {
                        String oliField = QuoteSyncUtil.getQuoteLineFieldMapTo(qliField);
                        Object oliValue = oli.get(oliField);
                        Object qliValue = qli.get(qliField);
                        
                        // boolean field is equal even not sync because of trigger on QuoteLineItem creation copy values from OppLineItem 
                        if (sync || (oliValue instanceof Boolean && qliValue instanceof Boolean && isCreate)) {
                            System.assertEquals(oliValue, qliValue, 'OpportunityLineItem.' + oliField + ' value does not match QuoteLineItem.' + qliField + ' value');
                        } else {
                           System.assertNotEquals(oliValue, qliValue, 'OpportunityLineItem.' + oliField + ' value should not match QuoteLineItem.' + qliField + ' value');                        
                        }    
                    }
                    
                    match = true; 
                    break;
                }
            }
            if (sync) {
                System.assert(match, 'No matching oppline');                       
            }             
        }
    }
    
    private static void assertQuoteFields(String quoteId, Quote expectedQuote) {
        String quote_fields = QuoteSyncUtil.getQuoteFieldsString();
        String quoteQuery = 'select Id, OpportunityId' + quote_fields + ' from Quote where Id = \'' + quoteId + '\'';
        
        Quote quote = Database.query(quoteQuery);
        
        for (String quoteField : QuoteSyncUtil.getQuoteFields()) {
            Object quoteValue = quote.get(quoteField); 
            Object expectedValue = expectedQuote.get(quoteField);
            System.assertEquals(expectedValue, quoteValue, 'Quote.' + quoteField + ' value does not match'); 
        }
    }
    
    private static void assertOppFields(String oppId, Opportunity expectedOpp) {
        String opp_fields = QuoteSyncUtil.getOppFieldsString();        
        String oppQuery = 'select Id, SyncedQuoteId' + opp_fields + ' from Opportunity where Id = \'' + oppId + '\'';    
        
        Opportunity opp = Database.query(oppQuery);
        
        for (String oppField : QuoteSyncUtil.getOppFields()) {
            Object oppValue = opp.get(oppField); 
            Object expectedValue = expectedOpp.get(oppField);
            System.assertEquals(expectedValue, oppValue, 'Opportunity.' + oppField + ' value does not match'); 
        }                
    }
    
    private static void assertQuoteLineFields(String quoteId, List<QuoteLineItem> quoteLines) {
        String qliFields = QuoteSyncUtil.getQuoteLineFieldsString();            
        String qliQuery = 'select Id, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + qliFields + ' from QuoteLineItem where QuoteId = \'' + quoteId + '\'';

        List<QuoteLineItem> qlis = Database.query(qliQuery);    
        
        System.assertEquals(quoteLines.size(), qlis.size(), 'Number of line items does not match');
        
        for (QuoteLineItem qli : qlis) {
            for (QuoteLineItem qline : quoteLines) {
                if (qli.Id == qline.Id) {
                    for (String qliField : QuoteSyncUtil.getQuoteLineFields()) {                    
                        Object qliValue = qli.get(qliField);
                        Object expectedValue = qline.get(qliField); 
                                                
                        System.assertEquals(expectedValue, qliValue, 'QuoteLineItem.' + qliField + ' value does not match');
                    }                        
                    break;
                }    
            }
        }
    }
    
    private static void assertOppLineFields(String oppId, List<OpportunityLineItem> oppLines) {
        String oliFields = QuoteSyncUtil.getOppLineFieldsString();            
        String oliQuery = 'select Id, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + oliFields + ' from OpportunityLineItem where OpportunityId = \'' + oppId + '\'';

        List<OpportunityLineItem> olis = Database.query(oliQuery);    
        
        System.assertEquals(oppLines.size(), olis.size(), 'Number of line items does not match');
        
        for (OpportunityLineItem oli : olis) {
            for (OpportunityLineItem oppline : oppLines) {
                if (oli.Id == oppline.Id) {
                    for (String oliField : QuoteSyncUtil.getOppLineFields()) {                    
                        Object oliValue = oli.get(oliField);
                        Object expectedValue = oppline.get(oliField);  
                        
                        System.assertEquals(expectedValue, oliValue, 'Opportunity.' + oliField + ' value does not match');
                    }                        
                    break;
                }    
            }
        }    
    }
    
    private static void printOpp(String oppId) {
        String opp_fields = QuoteSyncUtil.getOppFieldsString();        
        String oppQuery = 'select Id, SyncedQuoteId' + opp_fields + ' from Opportunity where Id = \'' + oppId + '\'';    
        
        Opportunity opp = Database.query(oppQuery);
        String oppValues = 'OpportunityId: ' + opp.Id;
        
        for (String oppField : QuoteSyncUtil.getOppFields()) {
            oppValues = ', ' + opp.get(oppField); 
        }  
        
        System.debug(oppValues);                
    }
    
    private static void printQuote(String quoteId) {
        String quote_fields = QuoteSyncUtil.getQuoteFieldsString();
        String quoteQuery = 'select Id, OpportunityId' + quote_fields + ' from Quote where Id = \'' + quoteId + '\'';
        
        Quote quote = Database.query(quoteQuery);
        String quoteValues = 'QuoteId: ' + quote.Id;
        
        for (String quoteField : QuoteSyncUtil.getQuoteFields()) {
            quoteValues = ', ' + quote.get(quoteField);
        }
        
        System.debug(quoteValues);    
    }
    
    private static void printOppLines(String oppId) {
      
        String oliFields =  QuoteSyncUtil.getOppLineFieldsString();
        String oliQuery = 'select Id, OpportunityId, PricebookEntryId, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + oliFields + ' from OpportunityLineItem where OpportunityId  = \'' + oppId + '\'';        

        List<OpportunityLineItem> olis = Database.query(oliQuery);
            
        Integer n = 0;
        for (OpportunityLineItem oli : olis) {
            n++;
            String oliValues = '*******************   OppLine ' + n + ': ' + oli.PricebookEntryId + ', ' + oli.UnitPrice + ', ' + oli.Quantity + ', ' + oli.Discount + ', ' + oli.ServiceDate;
            
            for (String oliField : QuoteSyncUtil.getOppLineFields()) {
                oliValues += ', ' + oli.get(oliField);
            }
            
            System.debug(oliValues);
        }    
    } 
    
    private static void printQuoteLines(String quoteId) {
   
        String qliFields = QuoteSyncUtil.getQuoteLineFieldsString();           
        String qliQuery = 'select Id, QuoteId, PricebookEntryId, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + qliFields + ' from QuoteLineItem where QuoteId = \'' + quoteId + '\'';
        
        List<QuoteLineItem> qlis = Database.query(qliQuery);
                        
        Integer n = 0;
        for (QuoteLineItem qli : qlis) {
            n++;
            String qliValues = '*******************   QuoteLine ' + n + ': ' + qli.PricebookEntryId + ', ' + qli.UnitPrice + ', ' + qli.Quantity + ', ' + qli.Discount + ', ' + qli.ServiceDate;
            
            for (String qliField : QuoteSyncUtil.getQuoteLineFields()) {
                qliValues += ', ' + qli.get(qliField);
            }
                                
            System.debug(qliValues);                
        }  
    }  

    private static void populateField(SObject sobj, String entity, String field) {
        List<String> fields = new List<String>();
        fields.add(field);
        populateFields(sobj, entity, fields);
    }

    private static void populateFields(SObject sobj, String entity, List<String> fields) {
        populateFields(sobj, entity, fields, false);
    }
   
    private static void populateFields(SObject sobj, String entity, Set<String> fields, boolean setNullOrFalse) {
        for (String field : fields) {
            Schema.DescribeFieldResult result = QuoteSyncUtil.getField(entity, field);
            sobj.put(field, QuoteSyncUtil.createValue(sobj, entity, field, result, setNullOrFalse));
        }
    }
    
    private static void populateFields(SObject sobj, String entity, List<String> fields, boolean setNullOrFalse) {
        for (String field : fields) {
            Schema.DescribeFieldResult result = QuoteSyncUtil.getField(entity, field);
            sobj.put(field, QuoteSyncUtil.createValue(sobj, entity, field, result, setNullOrFalse));
        }    
    }    
 
    private static Opportunity createOpp() { 
        return createOpp(false);
    } 
        
    private static Opportunity createOpp(boolean setNullOrFalse) {                      
        Opportunity opp = new Opportunity(Name='test opp', Amount=100, TotalOpportunityQuantity=10, StageName='Prospecting', CloseDate=System.today(), Pricebook2Id=pb.Id, AccountId=acc.Id);
        populateFields(opp, 'Opportunity', QuoteSyncUtil.getOppFields(), setNullOrFalse);
        
        QuoteSyncUtil.populateRequiredFields(opp);
                            
        insert(opp);
        return opp;
    }
    
    private static List<OpportunityLineItem> createOppLines(Opportunity opp, Integer num) {
        return createOppLines(opp, num, false, false);
    }    
    
    private static List<OpportunityLineItem> createOppLines(Opportunity opp, Integer num, boolean unique, boolean setNullOrFalse) {
    
        List<OpportunityLineItem> olis = new List<OpportunityLineItem>();
        for (Integer i = 1; i <= num; i++) {
          
            PricebookEntry currPbe = pbe;
        
            if (unique)  {
                Product2 p = new Product2(Name='test product ' + i, IsActive=true);             
                insert(p);                            
        
                currPbe = new PricebookEntry(Pricebook2Id=opp.Pricebook2Id, Product2Id=p.Id, IsActive=true, UnitPrice=10);
                insert(currPbe);                   
            } 
        
            //Integer n = 1;
            //if (unique) n *= i;
            
            OpportunityLineItem oli = new OpportunityLineItem(OpportunityId=opp.Id, UnitPrice=i*10, Quantity=i, Discount=i, ServiceDate=System.today(), PricebookEntryId=currPbe.Id);         
            populateFields(oli, 'OpportunityLineItem', QuoteSyncUtil.getOppLineFields(), setNullOrFalse);
 
            QuoteSyncUtil.populateRequiredFields(oli);
                          
            olis.add(oli);  
        }                      
        insert(olis);
        return olis;
    }
    
    private static List<OpportunityLineItem> createOppLinesNoSync(Opportunity opp, Integer num) {
    
        List<OpportunityLineItem> olis = new List<OpportunityLineItem>();
        for (Integer i = 1; i <= num; i++) {     
            OpportunityLineItem oli = new OpportunityLineItem(OpportunityId=opp.Id, UnitPrice=i*10, Quantity=i*1, Discount=i*1, ServiceDate=System.today(), PricebookEntryId=pbe.Id);
            
            QuoteSyncUtil.populateRequiredFields(oli);
                        
            olis.add(oli); 
             
        }                      
        insert(olis);
        return olis;
    }    

    private static Quote createQuote(Opportunity opp) {
        return createQuote(opp, false);
    }

    
    private static Quote createQuote(Opportunity opp, boolean setNullOrFalse) {
        Quote quote = new Quote(Name='test quote', OpportunityId=opp.Id, Pricebook2Id=opp.Pricebook2Id);
        populateFields(quote, 'Quote', QuoteSyncUtil.getQuoteFields(), setNullOrFalse);
                
        QuoteSyncUtil.populateRequiredFields(quote);
                       
        insert(quote);
        
        String quote_fields = QuoteSyncUtil.getQuoteFieldsString();
        String quoteQuery = 'select Id, Name, OpportunityId, Pricebook2Id' + quote_fields + ' from Quote where Id = \'' + quote.Id + '\'';
        
        quote = Database.query(quoteQuery);       
        return quote;
    }
   
    private static List<QuoteLineItem> createQuoteLines(Quote quote, Integer num) {
        return createQuoteLines(quote, num, false, false);    
    }   
    
    private static List<QuoteLineItem> createQuoteLines(Quote quote, Integer num, boolean unique, boolean setNullOrFalse) {
                 
        List<QuoteLineItem> qlis = new List<QuoteLineItem>();
        for (Integer i = 1; i <= num; i++) {
        
            PricebookEntry currPbe = pbe;
        
            if (unique)  {
                Product2 p = new Product2(Name='test product ' + i, IsActive=true);             
                insert(p);                            
        
                currPbe = new PricebookEntry(Pricebook2Id=quote.Pricebook2Id, Product2Id=p.Id, IsActive=true, UnitPrice=10);
                insert(currPbe);                   
            }
            
            //Integer n = 1 * i;
            //if (unique) n *= i;                     
        
            QuoteLineItem qli = new QuoteLineItem(QuoteId=quote.Id, UnitPrice=i*10, Quantity=i, Discount=i, ServiceDate=System.today(), PricebookEntryId=currPbe.Id);
            populateFields(qli, 'QuoteLineItem', QuoteSyncUtil.getQuoteLineFields(), setNullOrFalse);  
            
            QuoteSyncUtil.populateRequiredFields(qli);
                                         
            qlis.add(qli);           
        }        
        insert(qlis);
        return qlis;
    } 
    
    private static List<QuoteLineItem> createQuoteLinesNoSync(Quote quote, Integer num) {
                 
        List<QuoteLineItem> qlis = new List<QuoteLineItem>();
        for (Integer i = 1; i <= num; i++) {        
            QuoteLineItem qli = new QuoteLineItem(QuoteId=quote.Id, UnitPrice=i*10, Quantity=i*1, Discount=i*1, ServiceDate=System.today(), PricebookEntryId=pbe.Id);           
 
            QuoteSyncUtil.populateRequiredFields(qli);            
            
            qlis.add(qli);           
        }        
        insert(qlis);
        return qlis;
    }     

    private static Opportunity updateOpp(String oppId) {
        return updateOpp(oppId, false);
    }

    
    private static Opportunity updateOpp(String oppId, boolean updateNullOrFalse) {
       String opp_fields = QuoteSyncUtil.getOppFieldsString();
       String oppQuery = 'select Id' + opp_fields + ' from Opportunity where Id = \'' + oppId + '\'';
        
       Opportunity opp = Database.query(oppQuery);
       populateFields(opp, 'Opportunity', QuoteSyncUtil.getOppFields(), updateNullOrFalse);
         
       update(opp);
       return opp;                
    }
  
    private static Quote updateQuote(String quoteId) {
        return updateQuote(quoteId, false);
    }  
    
    private static Quote updateQuote(String quoteId, boolean updateNullOrFalse) {
       String quote_fields = QuoteSyncUtil.getQuoteFieldsString();
       String quoteQuery = 'select Id' + quote_fields + ' from Quote where Id = \'' + quoteId + '\'';
        
       Quote quote = Database.query(quoteQuery);
       populateFields(quote, 'Quote', QuoteSyncUtil.getQuoteFields(), updateNullOrFalse);
           
       update(quote);
       return quote;    
    }
    
    private static List<OpportunityLineItem> updateOppLines(String oppId) {
        return updateOppLines(oppId, true, true);
    }
    
    private static List<OpportunityLineItem> updateOppLinesStdSyncFields(String oppId) {
        return updateOppLines(oppId, true, false);
    }
    
    private static List<OpportunityLineItem> updateOppLinesCustomSyncFields(String oppId) {
        return updateOppLines(oppId, false, true);
    }
    
    private static List<OpportunityLineItem> updateOppLines(String oppId, boolean stdSyncField, boolean customSyncField) {
        return updateOppLines(oppId, stdSyncField, customSyncField, false);
    }                    
    
    private static List<OpportunityLineItem> updateOppLines(String oppId, boolean stdSyncField, boolean customSyncField, boolean updateNullOrFalse) {
    
        String oliFields =  QuoteSyncUtil.getOppLineFieldsString();
        String oliQuery = 'select Id, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + oliFields + ' from OpportunityLineItem where OpportunityId  = \'' + oppId + '\'';        

        List<OpportunityLineItem> olis = Database.query(oliQuery);
        
        for (OpportunityLineItem oli : olis) {
           
            if (stdSyncField) {
                oli.UnitPrice += 1;
                oli.Quantity += 1;
                oli.Discount += 1;
                oli.ServiceDate = oli.ServiceDate.addDays(1);    
            }                
            
            if (customSyncField) {
                populateFields(oli, 'OpportunityLineItem',  QuoteSyncUtil.getOppLineFields(), updateNullOrFalse);
             }   
        }
        update(olis);
        return olis;                   
    }
    
    private static List<QuoteLineItem> updateQuoteLines(String quoteId) {
        return updateQuoteLines(quoteId, true, true);
    }
    
    private static List<QuoteLineItem> updateQuoteLinesStdSyncFields(String quoteId) {
        return updateQuoteLines(quoteId, true, false);
    }
    
    private static List<QuoteLineItem> updateQuoteLinesCustomSyncFields(String quoteId) {
        return updateQuoteLines(quoteId, false, true);
    }           

    private static List<QuoteLineItem> updateQuoteLines(String quoteId, boolean stdSyncField, boolean customSyncField) {
        return updateQuoteLines(quoteId, stdSyncField, customSyncField, false); 
    }
        
    private static List<QuoteLineItem> updateQuoteLines(String quoteId, boolean stdSyncField, boolean customSyncField, boolean updateNullOrFalse) {
    
        String qliFields = QuoteSyncUtil.getQuoteLineFieldsString();            
        String qliQuery = 'select Id, UnitPrice, Quantity, Discount, ServiceDate, SortOrder' + qliFields + ' from QuoteLineItem where QuoteId = \'' + quoteId + '\'';

        List<QuoteLineItem> qlis = Database.query(qliQuery);

        for (QuoteLineItem qli : qlis) {

            if (stdSyncField) {        
                qli.UnitPrice += 1;
                qli.Quantity += 1;
                qli.Discount += 1;
                qli.ServiceDate = qli.ServiceDate.addDays(1);                   
            }             
        
            if (customSyncField) {
                populateFields(qli, 'QuoteLineItem', QuoteSyncUtil.getQuoteLineFields(), updateNullOrFalse);
           
            }    
        }
        update(qlis); 
        return qlis;        
    }
    
    private static Opportunity updateOppNoSync(String oppId) {
        String oppQuery = 'select Id, CloseDate from Opportunity where Id = \'' + oppId + '\'';
        
        Opportunity opp = Database.query(oppQuery);
        opp.CloseDate = System.today();       
        
        update(opp);
        return opp;                
    }    
    
    private static Quote updateQuoteNoSync(String quoteId) {
        String quoteQuery = 'select Id, ExpirationDate from Quote where Id = \'' + quoteId + '\'';
        
        Quote quote = Database.query(quoteQuery);
        quote.ExpirationDate = System.today();       
        
        update(quote);
        return quote;                
    }         

    private static List<OpportunityLineItem> updateOppLinesNoSync(String oppId, String field) {        
        String oliFields =  QuoteSyncUtil.getOppLineFieldsString();
        String oliQuery = 'select Id, ' + field + oliFields + ' from OpportunityLineItem where OpportunityId  = \'' + oppId + '\'';        
        
        if (field == null) {           
            oliQuery = 'select Id ' + oliFields + ' from OpportunityLineItem where OpportunityId  = \'' + oppId + '\'';        
        }

        List<OpportunityLineItem> olis = Database.query(oliQuery);
        
        if (field != null) {
            for (OpportunityLineItem oli : olis) {       
                populateField(oli, 'OpportunityLineItem', field); 
            }
            update(olis);
        }    
        return olis;                   
    }
    
    private static List<QuoteLineItem> updateQuoteLinesNoSync(String quoteId, String field) {
        String qliFields = QuoteSyncUtil.getQuoteLineFieldsString(); 
        String qliQuery = 'select Id, ' + field + qliFields + ' from QuoteLineItem where QuoteId = \'' + quoteId + '\'';
        
        if (field == null) {                     
            qliQuery = 'select Id ' + qliFields + ' from QuoteLineItem where QuoteId = \'' + quoteId + '\'';
        }

        List<QuoteLineItem> qlis = Database.query(qliQuery);

        if (field != null) {
            for (QuoteLineItem qli : qlis) {
                populateField(qli, 'QuoteLineItem', field);        
            }
            update(qlis); 
        }    
        return qlis;        
    }
    
        
    private static void startSync(String oppId, String quoteId) { 
        Opportunity opp = [select Id, SyncedQuoteId from Opportunity where Id = :oppId];
        opp.SyncedQuoteId = quoteId;
        update(opp);    
    }
    
    private static void stopSync(String oppId) {
        Opportunity opp = [select Id, SyncedQuoteId from Opportunity where Id = :oppId];
        opp.SyncedQuoteId = null;        
        update(opp);
    }
           
    
    /**
     * Create quote from opp. Verify quote sync fields are copied from opp
     */   
    static testMethod void testCreateQuote() {
        QuoteSyncUtil.isRunningTest = true;
        try {
            Opportunity opp = createOpp();
            Quote quote = createQuote(opp);
            assertQuoteNotSync(opp.Id, quote.Id, true);
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }    
    }
    
    /**
     * Create quote from opp with opp line. Verify quote line sync fields are copied from opp line
     */  
    static testMethod void testCreateQuoteWithQuoteLine() {
        QuoteSyncUtil.isRunningTest = true;
        try {    
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, 2, false, false);
            
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2, false, false);        
            
            //FIXME: Cannot force picklist value to be different
            //assertQuoteNotSync(opp.Id, quote.Id);
            //assertQuoteLineNotSync(opp.Id, quote.Id);        
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                     
    }    
    
    /**
     * Start sync from quote. Verify opp fields are sync'ed from quote
     */
    static testMethod void testStartSyncQuote() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
            Opportunity opp = createOpp();
            Quote quote = createQuote(opp);
            
            quote = updateQuote(quote.Id);
            assertQuoteNotSync(opp.Id, quote.Id);
            
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);        
            assertQuoteFields(quote.Id, quote); 
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                 
    }

    /**
     * Start sync from quote. Verify opp fields are sync'ed from quote
     */
    static testMethod void testStartSyncQuoteSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try {       
            Opportunity opp = createOpp();
            Quote quote = createQuote(opp, true);
                    
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);        
            assertQuoteFields(quote.Id, quote);
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                  
    }
    
    /**
     * Start sync from quote with quote line. Verify opp line fields are sync'ed from quote line
     */
    static testMethod void testStartSyncWithQuoteLine() {
        QuoteSyncUtil.isRunningTest = true;
        try {       
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
            
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
            
            quoteLines = updateQuoteLines(quote.Id);
           
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines); 
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                           
    }
 
    /**
     * Start sync from quote with quote line. Verify opp line fields are sync'ed from quote line
     */
    static testMethod void testStartSyncWithQuoteLineSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
            Opportunity opp = createOpp();
            //FIXME
            //List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
            
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2, false, true);
                    
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);                
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }               
    } 
   
    /**
     * Start sync from another quote. Verify opp and opp line fields are sync'ed from new quote and quote line
     */
    static testMethod void testStartSyncOtherQuote() {
        QuoteSyncUtil.isRunningTest = true;
        try {        
            Opportunity opp = createOpp();
    
            Quote quote1 = createQuote(opp);
            List<QuoteLineItem> quoteLines1 = createQuoteLines(quote1, 2);
            Quote quote2 = createQuote(opp);        
            List<QuoteLineItem> quoteLines2 = createQuoteLines(quote2, 2);
            
            startSync(opp.Id, quote1.Id);
            
            Quote updatedQuote = updateQuote(quote2.Id);
            List<QuoteLineItem> quoteLines = updateQuoteLines(quote2.Id); 
            
            startSync(opp.Id, quote2.Id);
            
            assertQuoteSync(opp.Id, quote2.Id);
            assertQuoteLineSync(opp.Id, quote2.Id);        
                    
            assertQuoteFields(quote2.Id, updatedQuote);  
            assertQuoteLineFields(quote2.Id, quoteLines);               
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }           
    }
    
    /**
     * Start sync from quote with quote lines create new opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncInsertOppLine() {
        QuoteSyncUtil.isRunningTest = true;
        try {        
            Opportunity opp = createOpp();
                  
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }           
    }
    
    /**
     * Start sync from quote with quote lines create new opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncInsertOppLineSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try {          
            Opportunity opp = createOpp();
                  
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2, false, true);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }         
    }    
    
    /**
     * Start sync from quote with quote lines update existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncUpdateOppLine() {
        QuoteSyncUtil.isRunningTest = true;
        try {    
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, 2);  
                    
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
            quoteLines = updateQuoteLines(quote.Id);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);    
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }          
    }
    
    /**
     * Start sync from quote with quote lines update existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncUpdateOppLineSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try {    
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, 2);  
                    
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
            quoteLines = updateQuoteLines(quote.Id, true, true, true);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);    
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }             
    }    
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_1() {
        QuoteSyncUtil.isRunningTest = true;
        try {       
            Integer numLine = 2;
           
            Opportunity opp = createOpp();
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
            
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);    
                    
            stopSync(opp.Id);        
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);
                        
            quoteLines = updateQuoteLinesCustomSyncFields(quote.Id);
                    
            //printOppLines(opp.Id);      
            //printQuoteLines(quote.Id);
                                        
            startSync(opp.Id, quote.Id);
            
            //printOppLines(opp.Id);      
            //printQuoteLines(quote.Id);        
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }          
    } 

   
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_1a() {
        QuoteSyncUtil.isRunningTest = true;
        try {           
            Integer numLine = 2;
           
            Opportunity opp = createOpp();
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
            
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);    
                    
            stopSync(opp.Id);        
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);    
                    
            updateOppLinesCustomSyncFields(opp.Id);
                
            //printOppLines(opp.Id);              
            //printQuoteLines(quote.Id);
                          
            startSync(opp.Id, quote.Id);
            
            //printOppLines(opp.Id);
            //printQuoteLines(quote.Id);
                    
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }          
    } 
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_2() {
        QuoteSyncUtil.isRunningTest = true;
        try {    
            Integer numLine = 2;
           
            Opportunity opp = createOpp();
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
            
            startSync(opp.Id, quote.Id);
            stopSync(opp.Id);        
            
            quoteLines = updateQuoteLinesStdSyncFields(quote.Id);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                    
    }
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_2a() {
        QuoteSyncUtil.isRunningTest = true;
        try {      
            Integer numLine = 2;
           
            Opportunity opp = createOpp();
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
            
            startSync(opp.Id, quote.Id);
            stopSync(opp.Id);        
            
            updateOppLinesStdSyncFields(opp.Id);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                
    }                                         
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_3() {
        QuoteSyncUtil.isRunningTest = true;
        try {      
            Integer numLine = 2;
           
            Opportunity opp = createOpp();
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
            
            startSync(opp.Id, quote.Id);
            stopSync(opp.Id);        
            
            quoteLines = updateQuoteLines(quote.Id);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }         
    }
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_3a() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
            Integer numLine = 2;
           
            Opportunity opp = createOpp();
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
            
            startSync(opp.Id, quote.Id);
    
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);            
            
            stopSync(opp.Id);        
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);    
            
            //FIXME
            //updateOppLines(opp.Id);
            
            //printOppLines(opp.Id);        
            //printQuoteLines(quote.Id);
                  
            startSync(opp.Id, quote.Id);
            
            //printOppLines(opp.Id);        
            //printQuoteLines(quote.Id);
                    
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                   
    }              
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_4() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
            Integer numLine = 2;    
        
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, numLine);        
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
                             
            quoteLines = updateQuoteLinesCustomSyncFields(quote.Id);
               
            //printOppLines(opp.Id);              
            //printQuoteLines(quote.Id);
                          
            startSync(opp.Id, quote.Id);
            
            //printOppLines(opp.Id);        
            //printQuoteLines(quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines); 
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }              
    }
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_4a() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
            Integer numLine = 2;    
        
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, numLine, true, false);        
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine, true, false);
                   
            quoteLines = updateQuoteLinesCustomSyncFields(quote.Id);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }   
    }
        

    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_5() {
        QuoteSyncUtil.isRunningTest = true;
        try { 
            Integer numLine = 2;    
        
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, numLine);        
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
                   
            quoteLines = updateQuoteLinesStdSyncFields(quote.Id);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }   
    }
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_6() {
        QuoteSyncUtil.isRunningTest = true;
        try { 
            Integer numLine = 2;    
        
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, numLine);        
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
                   
            quoteLines = updateQuoteLines(quote.Id);
                  
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        } 
    }    
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_7() {
        QuoteSyncUtil.isRunningTest = true;
        try {
            Integer numLine = 2;    
        
            Opportunity opp = createOpp();                     
            Quote quote = createQuote(opp);
            
            // create quotelines first so trigger doesn't copy boolean field value from opplines on create
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine);
            List<OpportunityLineItem> oppLines = createOppLines(opp, numLine);  
                                                    
            //printOppLines(opp.Id);        
            //printQuoteLines(quote.Id);
                                         
            startSync(opp.Id, quote.Id);
            
            //printOppLines(opp.Id);        
            //printQuoteLines(quote.Id);        
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        } 
    }
    
    /**
     * Start sync from quote with quote lines that match existing opp lines on opp.  Verify opp line fields are sync'ed
     */ 
    static testMethod void testStartSyncMatchOppLine_7a() {
        QuoteSyncUtil.isRunningTest = true;
        try {
            Integer numLine = 2;    
        
            Opportunity opp = createOpp();
            List<OpportunityLineItem> oppLines = createOppLines(opp, numLine, true, false);        
                     
            Quote quote = createQuote(opp);
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, numLine, true, false);
                                 
            startSync(opp.Id, quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);  
            
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }         
    }                                
        
    /**
     * Stop sync on verify no changes on opp & opp line, quote & quote line fields
     */
    static testMethod void testStopSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {
            Opportunity opp = createOpp();            
            Quote quote = createQuote(opp);
            
            // create quotelines first so trigger doesn't copy boolean field value from opplines on create
            List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);        
            List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
                                
            startSync(opp.Id, quote.Id);
    
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);         
            
            stopSync(opp.Id);        
            
            assertQuoteSync(opp.Id, quote.Id);
            assertQuoteLineSync(opp.Id, quote.Id);          
            assertQuoteLineFields(quote.Id, quoteLines);      
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }          
    }  
        
    /**
     * Update opp sync fields.  Verify corresponding fields on quote are updated.
     */
    static testMethod void testUpdateOppSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertOppFields(opp.Id, updatedOpp);                 
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }   
    }
    
    /**
     * Update opp sync fields.  Verify corresponding fields on quote are updated.
     */
    static testMethod void testUpdateOppSyncSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try { 
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id, true);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertOppFields(opp.Id, updatedOpp);  
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                         
    }      
    
    /**
     * Update opp line sync fields.  Verify corresponding fields on quote line are updated.
     */
    static testMethod void testUpdateOppLineSyncSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try {  
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
                  
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id);
           List<OpportunityLineItem> oppLines = updateOppLines(opp.Id, true, true, true);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppFields(opp.Id, updatedOpp);
           assertOppLineFields(opp.Id, oppLines);  
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                      
    } 
        
    /**
     * Update opp line sync fields.  Verify corresponding fields on quote line are updated.
     */
    static testMethod void testUpdateOppLineSync_1() {
        QuoteSyncUtil.isRunningTest = true;
        try {
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
                  
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id);
           List<OpportunityLineItem> oppLines = updateOppLines(opp.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppFields(opp.Id, updatedOpp);
           assertOppLineFields(opp.Id, oppLines);
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }                    
    } 
    
    /**
     * Update opp line sync fields.  Verify corresponding fields on quote line are updated.
     */
    static testMethod void testUpdateOppLineSync_2() {
        QuoteSyncUtil.isRunningTest = true;
        try {
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
                  
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id);
           List<OpportunityLineItem> oppLines = updateOppLinesStdSyncFields(opp.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppFields(opp.Id, updatedOpp);
           assertOppLineFields(opp.Id, oppLines);            
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }          
    }
    
    /**
     * Update opp line sync fields.  Verify corresponding fields on quote line are updated.
     */
    static testMethod void testUpdateOppLineSync_3() {
        QuoteSyncUtil.isRunningTest = true;
        try {    
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
                  
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id);
           List<OpportunityLineItem> oppLines = updateOppLinesCustomSyncFields(opp.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppFields(opp.Id, updatedOpp);
           assertOppLineFields(opp.Id, oppLines);            
        } finally {
            QuoteSyncUtil.isRunningTest = false;
        }             
    }           
   
    /**
     * Insert opp line with sync fields set.  Verify corresponding fields on quote line are sync'ed.
     */
    static testMethod void testInsertOppLineSync() {
       QuoteSyncUtil.isRunningTest = true; 
       try { 
           Opportunity opp = createOpp();
           List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
           
           Quote quote = createQuote(opp);
                        
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id);
           oppLines = createOppLines(opp, 2);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppFields(opp.Id, updatedOpp);
           assertOppLineFields(opp.Id, oppLines);
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }         
    }     
    
    
    /**
     * Update quote sync fields.  Verify corresponding fields on opp are updated.
     */
    static testMethod void testUpdateQuoteSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {        
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           Quote updatedQuote = updateQuote(quote.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteFields(quote.Id, updatedQuote);   
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                         
    } 
    
    /**
     * Update quote sync fields.  Verify corresponding fields on opp are updated.
     */
    static testMethod void testUpdateQuoteSyncSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try {
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           Quote updatedQuote = updateQuote(quote.Id, true);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteFields(quote.Id, updatedQuote);
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                         
    }     
    
    /**
     * Update quote line sync fields.  Verify corresponding fields on opp line are updated.
     */
    static testMethod void testUpdateQuoteLineSync_1() {
        QuoteSyncUtil.isRunningTest = true;
        try { 
           Opportunity opp = createOpp();
    
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);       
                  
           startSync(opp.Id, quote.Id); 
           
           Quote updatedQuote = updateQuote(quote.Id);
           List<QuoteLineItem> updatedQuoteLines = updateQuoteLines(quote.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteFields(quote.Id, updatedQuote);
           assertQuoteLineFields(quote.Id, updatedQuoteLines);
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                    
    }
    
    /**
     * Update quote line sync fields.  Verify corresponding fields on opp line are updated.
     */
    static testMethod void testUpdateQuoteLineSyncSetNull() {
        QuoteSyncUtil.isRunningTest = true;
        try {  
           Opportunity opp = createOpp();
    
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);       
                  
           startSync(opp.Id, quote.Id); 
           
           Quote updatedQuote = updateQuote(quote.Id);
           List<QuoteLineItem> updatedQuoteLines = updateQuoteLines(quote.Id, true, true, true);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteFields(quote.Id, updatedQuote);
           assertQuoteLineFields(quote.Id, updatedQuoteLines);   
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                
    }    
    
    /**
     * Update quote line sync fields.  Verify corresponding fields on opp line are updated.
     */
    static testMethod void testUpdateQuoteLineSync_2() {
        QuoteSyncUtil.isRunningTest = true;
        try {    
           Opportunity opp = createOpp();
    
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);       
                  
           startSync(opp.Id, quote.Id); 
           
           Quote updatedQuote = updateQuote(quote.Id);
           List<QuoteLineItem> updatedQuoteLines = updateQuoteLinesStdSyncFields(quote.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteFields(quote.Id, updatedQuote);
           assertQuoteLineFields(quote.Id, updatedQuoteLines);  
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                   
    }
    
    /**
     * Update quote line sync fields.  Verify corresponding fields on opp line are updated.
     */
    static testMethod void testUpdateQuoteLineSync_3() {
        QuoteSyncUtil.isRunningTest = true;
        try {      
           Opportunity opp = createOpp();
    
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);       
                  
           startSync(opp.Id, quote.Id); 
           
           Quote updatedQuote = updateQuote(quote.Id);
           List<QuoteLineItem> updatedQuoteLines = updateQuoteLinesCustomSyncFields(quote.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteFields(quote.Id, updatedQuote);
           assertQuoteLineFields(quote.Id, updatedQuoteLines);
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                    
    }                      

    /**
     * Insert quote line with sync fields.  Verify corresponding fields on opp line are sync'ed.
     */
    static testMethod void testInsertQuoteLineSync() {
       QuoteSyncUtil.isRunningTest = true;
       try {       
           Opportunity opp = createOpp();
           List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           Quote updatedQuote = updateQuote(quote.Id);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteFields(quote.Id, updatedQuote);
           assertQuoteLineFields(quote.Id, quoteLines);
       } finally {
          QuoteSyncUtil.isRunningTest = false;       
       }            
    }                        
      
    /**
     * Update opp non-sync fields.  Verify fields on quote are not updated.
     */
    static testMethod void testUpdateOppNoSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
            Opportunity opp = createOpp();
            Quote quote = createQuote(opp);
            
            startSync(opp.Id, quote.Id);
            
            updateOppNoSync(opp.Id);
            
            assertQuoteSync(opp.Id, quote.Id);        
            assertQuoteFields(quote.Id, quote); 
        } finally {
           QuoteSyncUtil.isRunningTest = false;       
        }                
    } 
        
    /**
     * Update opp line non-sync fields.  Verify fields on quote line are not updated.
     */
    static testMethod void testUpdateOppLineNoSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {    
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
                  
           startSync(opp.Id, quote.Id); 
           
           // remove sync field
           String field = QuoteSyncUtil.removeQuoteLineField('description');
    
           try {
               List<OpportunityLineItem> oppLines = updateOppLinesNoSync(opp.Id, field);
        
               assertQuoteSync(opp.Id, quote.Id);
               assertQuoteLineSync(opp.Id, quote.Id);  
               
               assertOppLineFields(opp.Id, oppLines); 
           } finally {
               // add sync field
               if (field != null)
                   QuoteSyncUtil.addQuoteLineField('description', field);
           } 
        } finally {
           QuoteSyncUtil.isRunningTest = false;       
        }                                
    } 
   
    /**
     * Insert opp line without sync fields.  Verify fields on quote line are sync'ed.
     */
    static testMethod void testInsertOppLineNoSync() {
       QuoteSyncUtil.isRunningTest = true;
       try {
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           List<OpportunityLineItem> oppLines = createOppLinesNoSync(opp, 2);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppLineFields(opp.Id, oppLines);
       } finally {         
           QuoteSyncUtil.isRunningTest = false;
       }        
    }
    
    /**
     * Update quote non-sync fields.  Verify corresponding fields on opp are not updated.
     */
    static testMethod void testUpdateQuoteNoSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
            Opportunity opp = createOpp();
            Quote quote = createQuote(opp);
            
            startSync(opp.Id, quote.Id);
            
            updateQuoteNoSync(quote.Id);
            
            assertQuoteSync(opp.Id, quote.Id);        
            assertQuoteFields(quote.Id, quote);
        } finally {         
           QuoteSyncUtil.isRunningTest = false;
        }                 
    } 
    
    /**
     * Update quote line non-sync fields.  Verify corresponding fields on opp line are not updated.
     */
    static testMethod void testUpdateQuoteLineNoSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
                  
           startSync(opp.Id, quote.Id); 
           
           // remove sync field
           String field = QuoteSyncUtil.removeQuoteLineField('description');
           try {
               quoteLines = updateQuoteLinesNoSync(quote.Id, 'description');
        
               assertQuoteSync(opp.Id, quote.Id);
               assertQuoteLineSync(opp.Id, quote.Id);  
               
               assertQuoteLineFields(quote.Id, quoteLines); 
           } finally {
               // add sync field
               if (field != null)
                   QuoteSyncUtil.addQuoteLineField('description', field);
           } 
        } finally {         
           QuoteSyncUtil.isRunningTest = false;
        }                             
    }          

    /**
     * Insert quote line without sync fields.  Verify corresponding fields on opp line are not sync'ed.
     */
    static testMethod void testInsertQuoteLineNoSync() {
       QuoteSyncUtil.isRunningTest = true;
       try {    
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           List<QuoteLineItem> quoteLines = createQuoteLinesNoSync(quote, 2);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteLineFields(quote.Id, quoteLines); 
       } finally {
           QuoteSyncUtil.isRunningTest = false;
       }            
    } 
    
    /**
     * Update opp when it's not sync with quote.
     */
    static testMethod void testUpdateOppNotSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                         
           Opportunity updatedOpp = updateOpp(opp.Id);
           
           assertQuoteNotSync(opp.Id, quote.Id);
           assertOppFields(opp.Id, updatedOpp); 
        } finally {
           QuoteSyncUtil.isRunningTest = false;
        }                  
    }
  
    /**
     * Update quote when it's not sync with opp.
     */
    static testMethod void testUpdateQuoteNotSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {       
           Opportunity opp = createOpp();
           Quote quote = createQuote(opp);
                         
           Quote updatedQuote = updateQuote(quote.Id);
           
           assertQuoteNotSync(opp.Id, quote.Id);
           assertQuoteFields(quote.Id, updatedQuote);
        } finally {
           QuoteSyncUtil.isRunningTest = false;
        }                 
    } 
   
    /**
     * Update opp line when opp not sync with quote.
     */
    static testMethod void testUpdateOppLineNotSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {      
           Opportunity opp = createOpp();
           List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
           
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);       
                   
           Opportunity updatedOpp = updateOpp(opp.Id);
           List<OpportunityLineItem> updatedOppLines = updateOppLines(opp.Id);
           
           assertQuoteNotSync(opp.Id, quote.Id);
           assertQuoteLineNotSync(opp.Id, quote.Id);  
           
           assertOppFields(opp.Id, updatedOpp);
           assertOppLineFields(opp.Id, updatedOppLines);
        } finally {
           QuoteSyncUtil.isRunningTest = false;
        }                
    }
  
    /**
     * Update quote line when quote not sync with opp.
     */
    static testMethod void testUpdateQuoteLineNotSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
           Opportunity opp = createOpp();
           List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
           
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);       
                        
           Quote updatedQuote = updateQuote(quote.Id);
           List<QuoteLineItem> updatedQuoteLines = updateQuoteLines(quote.Id);
           
           assertQuoteNotSync(opp.Id, quote.Id);
           assertQuoteLineNotSync(opp.Id, quote.Id);  
           
           assertQuoteFields(quote.Id, updatedQuote);
           assertQuoteLineFields(quote.Id, updatedQuoteLines); 
        } finally {
           QuoteSyncUtil.isRunningTest = false;
        }                 
    }  
    
    /**
     * Insert opp line when opp not sync with quote.
     */
    static testMethod void testInsertOppLineNotSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
           Opportunity opp = createOpp();
           List<OpportunityLineItem> oppLines = createOppLines(opp, 2);
           
           Quote quote = createQuote(opp);
                   
           oppLines.addAll(createOppLines(opp, 2));
           
           assertQuoteNotSync(opp.Id, quote.Id, true);
           assertQuoteLineNotSync(opp.Id, quote.Id, true);
                  
           assertOppLineFields(opp.Id, oppLines);
        } finally {
           QuoteSyncUtil.isRunningTest = false;
        }                       
    }
  
    /**
     * Insert quote line when quote not sync with opp.
     */
    static testMethod void testInsertQuoteLineNotSync() {
        QuoteSyncUtil.isRunningTest = true;
        try {        
           Opportunity opp = createOpp();
           
           Quote quote = createQuote(opp);                    
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2);
           
           assertQuoteNotSync(opp.Id, quote.Id, true);
           assertQuoteLineNotSync(opp.Id, quote.Id, true);
                 
           assertQuoteLineFields(quote.Id, quoteLines);
        } finally {
           QuoteSyncUtil.isRunningTest = false;
        }           
    }
    
    /**
     * Create duplicate opp line with from same pricebookentry
     */ 
    static testMethod void testCreateDuplicateOppLine() {
       QuoteSyncUtil.isRunningTest = true;
       try {     
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           List<OpportunityLineItem> oppLines = createOppLines(opp, 2, false, false);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppLineFields(opp.Id, oppLines);
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                
    } 
    
    /**
     * Create duplicate quote line with from same pricebookentry
     */ 
    static testMethod void testCreateDuplicateQuoteLine() {
       QuoteSyncUtil.isRunningTest = true;
       try {         
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
                  
           startSync(opp.Id, quote.Id); 
           
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2, false, false);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteLineFields(quote.Id, quoteLines);  
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                  
    }
    
    /**
     * Update duplicate opp line with from same pricebookentry
     */ 
    static testMethod void testUpdateDuplicateOppLine() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2, false, false);
                  
           startSync(opp.Id, quote.Id); 
           
           Opportunity updatedOpp = updateOpp(opp.Id);
           List<OpportunityLineItem> oppLines = updateOppLines(opp.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertOppFields(opp.Id, updatedOpp);
           assertOppLineFields(opp.Id, oppLines);
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }              
    } 
    
    /**
     * Update duplicate quote line with from same pricebookentry
     */ 
    static testMethod void testUpdateDuplicateQuoteLine() {
        QuoteSyncUtil.isRunningTest = true;
        try {     
           Opportunity opp = createOpp();
     
           Quote quote = createQuote(opp);
           List<QuoteLineItem> quoteLines = createQuoteLines(quote, 2, false, false);
                  
           startSync(opp.Id, quote.Id); 
           
           quote = updateQuote(quote.Id);
           quoteLines = updateQuoteLines(quote.Id);
           
           assertQuoteSync(opp.Id, quote.Id);
           assertQuoteLineSync(opp.Id, quote.Id);  
           
           assertQuoteFields(quote.Id, quote);
           assertQuoteLineFields(quote.Id, quoteLines);
       } finally {
           QuoteSyncUtil.isRunningTest = false;       
       }                
    }                                                         
}