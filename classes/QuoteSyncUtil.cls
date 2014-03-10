public class QuoteSyncUtil {

    public static boolean isRunningTest = false;  

    private static Map<String, String> quoteFieldMap = new Map<String, String>();
    private static Map<String, String> quoteLineFieldMap = new Map<String, String>();
    private static Set<Id> newQuoteIds = new Set<Id>();
    
    
    static {
        /*
        quoteFieldMap.put('description', 'description');
        quoteFieldMap.put('custom_text__c', 'custom_text__c');  
        quoteFieldMap.put('custom_checkbox__c', 'custom_checkbox__c');         
        quoteFieldMap.put('custom_currency__c', 'custom_currency__c');
        quoteFieldMap.put('custom_date__c', 'custom_date__c');              
        quoteFieldMap.put('custom_datetime__c', 'custom_datetime__c'); 
        quoteFieldMap.put('custom_email__c', 'custom_email__c');          
        quoteFieldMap.put('custom_multipicklist__c', 'custom_multipicklist__c'); 
        quoteFieldMap.put('custom_number__c', 'custom_number__c'); 
        quoteFieldMap.put('custom_percent__c', 'custom_percent__c'); 
        quoteFieldMap.put('custom_phone__c', 'custom_phone__c');      
        quoteFieldMap.put('custom_picklist__c', 'custom_picklist__c'); 
        quoteFieldMap.put('custom_rta__c', 'custom_rta__c'); 
        quoteFieldMap.put('custom_url__c', 'custom_url__c'); 
                       
                              
        quoteLineFieldMap.put('description', 'description');
        quoteLineFieldMap.put('custom_text__c', 'custom_text__c');
        quoteLineFieldMap.put('custom_checkbox__c', 'custom_checkbox__c');         
        quoteLineFieldMap.put('custom_currency__c', 'custom_currency__c');
        quoteLineFieldMap.put('custom_date__c', 'custom_date__c');              
        quoteLineFieldMap.put('custom_datetime__c', 'custom_datetime__c'); 
        quoteLineFieldMap.put('custom_email__c', 'custom_email__c');          
        quoteLineFieldMap.put('custom_multipicklist__c', 'custom_multipicklist__c'); 
        quoteLineFieldMap.put('custom_number__c', 'custom_number__c'); 
        quoteLineFieldMap.put('custom_percent__c', 'custom_percent__c'); 
        quoteLineFieldMap.put('custom_phone__c', 'custom_phone__c');      
        quoteLineFieldMap.put('custom_picklist__c', 'custom_picklist__c'); 
        quoteLineFieldMap.put('custom_rta__c', 'custom_rta__c'); 
        quoteLineFieldMap.put('custom_url__c', 'custom_url__c');        
        */

        Map<String, QuoteSyncField__c> quoteFields = QuoteSyncField__c.getAll();
        for (String quoteField : quoteFields.keySet()) {
            QuoteSyncField__c quoteSyncField = QuoteSyncField__c.getInstance(quoteField);
            //FIXME - uppercase value cause NPE
            //quoteFieldMap.put(quoteSyncField.Name, quoteSyncField.OppSyncField__c);
            quoteFieldMap.put(quoteField, quoteSyncField.OppSyncField__c);
        }
        
        Map<String, QuoteLineSyncField__c> quoteLineFields = QuoteLineSyncField__c.getAll();
        for (String quoteLineField : quoteLineFields.keySet()) {
            QuoteLineSyncField__c quoteLineSyncField = QuoteLineSyncField__c.getInstance(quoteLineField);
            //FIXME - uppercase value cause NPE
            //quoteLineFieldMap.put(quoteLineSyncField.Name, quoteLineSyncField.OppLineSyncField__c);
            quoteLineFieldMap.put(quoteLineField, quoteLineSyncField.OppLineSyncField__c);            
        }
    }
    
    public static Set<Id> getNewQuoteIds() {
        return newQuoteIds;
    }
    
    public static void addNewQuoteId(Id id) {
        newQuoteIds.add(id);
    }
  
    public static void removeNewQuoteId(Id id) {
        newQuoteIds.remove(id);
    }
    
    public static void removeAllNewQuoteIds(Set<Id> ids) {
        newQuoteIds.removeAll(ids);
    }
    
    public static void clearNewQuoteIds() {
        newQuoteIds.clear();
    }
    
    public static Boolean isNewQuote(Id id) {
        return newQuoteIds.contains(id);
    }
    
    public static Set<String> getQuoteLineFields() {        
        return quoteLineFieldMap.keySet();
    }

    public static Set<String> getQuoteFields() { 
        return quoteFieldMap.keySet();
    }
    
    public static List<String> getOppLineFields() {
        return quoteLineFieldMap.values();
    }

    public static List<String> getOppFields() {
        return quoteFieldMap.values();
    }    
    
    public static String getQuoteFieldMapTo(String quoteField) {
        return quoteFieldMap.get(quoteField);
    } 
    
    public static String getQuoteLineFieldMapTo(String quoteLineField) {
        return quoteLineFieldMap.get(quoteLineField);
    }

    public static String getQuoteFieldsString() {    
        String fields = '';
        for (String field : getQuoteFields()) {
            fields += ', ' + field;
        }
        return fields; 
    }

    public static String getOppFieldsString() {    
        String fields = '';
        for (String field : getOppFields()) {
            fields += ', ' + field;
        }
        return fields; 
    }
    
    public static String getQuoteLineFieldsString() {    
        String fields = '';
        for (String field : getQuoteLineFields()) {
            fields += ', ' + field;
        }
        return fields; 
    }

    public static String getOppLineFieldsString() {    
        String fields = '';
        for (String field : getOppLineFields()) {
            fields += ', ' + field;
        }
        return fields; 
    }
    
    public static String addQuoteField(String quoteField, String oppField) {
        return quoteFieldMap.put(quoteField, oppField);
    }

    public static String addQuoteLineField(String qliField, String oliField) {
        return quoteLineFieldMap.put(qliField, oliField);
    } 
    
    public static String removeQuoteField(String quoteField) {
        return quoteFieldMap.remove(quoteField);
    }

    public static String removeQuoteLineField(String qliField) {
        return quoteLineFieldMap.remove(qliField);
    } 
    
    private static Map<String, SObjectField> quoteFields = Schema.getGlobalDescribe().get('Quote').getDescribe().fields.getMap();
    private static Map<String, SObjectField> quoteLineFields = Schema.getGlobalDescribe().get('QuoteLineItem').getDescribe().fields.getMap();

    private static Map<String, SObjectField> oppFields = Schema.getGlobalDescribe().get('Opportunity').getDescribe().fields.getMap();
    private static Map<String, SObjectField> oppLineFields = Schema.getGlobalDescribe().get('OpportunityLineItem').getDescribe().fields.getMap();
     
    public static Schema.DescribeFieldResult getField(String entity, String field) {
        Schema.DescribeFieldResult f = null;    
    
        if (entity == 'Quote') {
            f = quoteFields.get(field).getDescribe();
        } else if (entity == 'QuoteLineItem') {
            f = quoteLineFields.get(field).getDescribe();
        } else if (entity == 'Opportunity') {
            f = oppFields.get(field).getDescribe();
        } else if (entity == 'OpportunityLineItem') {
            f = oppLineFields.get(field).getDescribe();
        } else {
            System.assert(false, 'Unknown entity: ' + entity + ', field: ' + field);
        }
        
        return f;    
    }      
    
    public static Map<String, Schema.DescribeFieldResult> QuoteRequiredFields = getCustomRequiredFields('Quote');
    public static Map<String, Schema.DescribeFieldResult> QuoteLineRequiredFields = getCustomRequiredFields('QuoteLineItem');
    public static Map<String, Schema.DescribeFieldResult> OppRequiredFields = getCustomRequiredFields('Opportunity');
    public static Map<String, Schema.DescribeFieldResult> OppLineRequiredFields = getCustomRequiredFields('OpportunityLineItem'); 
 
    
    public static Map<String,  Schema.DescribeFieldResult> getCustomRequiredFields(String entity) {
        Map<String, SObjectField> fields = null;
        Set<String> excludeFields = new Set<String>(); 

        if (entity == 'Quote') {
            fields = quoteFields;
            excludeFields.addAll(getQuoteFields());
        } else if (entity == 'QuoteLineItem') {
            fields = quoteLineFields;
            excludeFields.addAll(getQuoteLineFields());
        } else if (entity == 'Opportunity') {
            fields = oppFields;
            excludeFields.addAll(getOppFields());            
        } else if (entity == 'OpportunityLineItem') {
            fields = oppLineFields;
            excludeFields.addAll(getOppLineFields());             
        } else {
            System.assert(false, 'Unknown entity: ' + entity);
        }
        
        Map<String, Schema.DescribeFieldResult> requiredFields = new Map<String,  Schema.DescribeFieldResult>(); 
        
        for (String field : fields.keySet()) {
            Schema.DescribeFieldResult f = fields.get(field).getDescribe();
                                
            if (excludeFields.contains(field) && f.getType() != DisplayType.Boolean) continue;
                                
            if (f.isCustom() && f.isCreateable() && !f.isNillable() && !f.isAutoNumber() && !f.isCalculated() && 
               (!f.isDefaultedOnCreate() || f.getType() == DisplayType.Boolean)) {
               requiredFields.put(field, f);         
            }                
        } 
        
        return requiredFields;             
    } 
    
    public static void populateRequiredFields(SObject sobj) {
        String entity = sobj.getSObjectType().getDescribe().getName(); 
    
        Map<String, Schema.DescribeFieldResult> fields = null;
 
       if (entity == 'Quote') {
            fields = QuoteRequiredFields;
        } else if (entity == 'QuoteLineItem') {
            fields = QuoteLineRequiredFields;
        } else if (entity == 'Opportunity') {
            fields = OppRequiredFields;         
        } else if (entity == 'OpportunityLineItem') {
            fields = OppLineRequiredFields;       
        } else {
            System.assert(false, 'Unknown entity: ' + entity);
        }    
    
    
        for (String field : fields.keySet()) {
            if (sobj.get(field) != null) continue;
            Schema.DescribeFieldResult result = fields.get(field); 
            sobj.put(field, createValue(sobj, entity, field, result, false));
        }
    }
    
    public static Object createValue(SObject sobj, String entity, String field, Schema.DescribeFieldResult result, boolean setNullOrFalse) {
    
        Object value = null;
        String strValue = entity + '_' + field + '_' + System.currentTimeMillis();
        
        Schema.DisplayType ftype = result.getType();
        
        if (!setNullOrFalse) {
        
            if (ftype == DisplayType.Base64) {
                value = Blob.valueOf(strValue);
            } else if (ftype == DisplayType.Boolean) {
                if (sobj.get(field) != null) {
                   value = !Boolean.valueOf(sobj.get(field));
                } else if (entity == 'Quote' || entity == 'QuoteLineItem') {
                   value = false; 
                }  else {
                    value = true;
                }
            } else if (ftype == DisplayType.Currency) {
                value = Decimal.valueOf(Math.random());
            } else if (ftype == DisplayType.Date) {
                value = Date.today();
            } else if (ftype == DisplayType.DateTime) {
                value = DateTime.now();
            } else if (ftype == DisplayType.Integer) {
                value = Integer.valueOf(Math.random());
            } else if (ftype == DisplayType.Double || ftype == DisplayType.Percent) {
                value = Math.random();
            } else if (ftype == DisplayType.Email) {
                value =  System.currentTimeMillis() + '@quotesync.test';
            } else if (ftype == DisplayType.Phone) {
                value = String.valueOf(System.currentTimeMillis());
            } else if (ftype == DisplayType.Picklist) {
                List <Schema.PicklistEntry> values = result.getPicklistValues(); 
                value = values.get(0).getValue();
            } else if (ftype == DisplayType.MultiPicklist) {
                List <Schema.PicklistEntry> values = result.getPicklistValues(); 
                value = values.get(0).getValue();            
            } else if (ftype == DisplayType.String || 
                       ftype == DisplayType.TextArea ||
                       ftype == DisplayType.Url) {
                value = strValue;
            } else {
                value = strValue;
            }
        } else {
            if (ftype == DisplayType.Boolean) {
                value = false;
            }
        
        }    
        
        return value;        
    }                                 
}