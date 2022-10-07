DELETE FROM conversation_references WHERE parent_id IN (SELECT parent_id FROM conversation_references EXCEPT SELECT id FROM conversations);
select parent_id from conversation_references EXCEPT select id from conversations; 
