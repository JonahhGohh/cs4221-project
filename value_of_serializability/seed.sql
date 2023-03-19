do $$
declare 
   counter integer := 1;
begin
   while counter <= 1000000 loop
      insert into serializability_1 (a, b) VALUES (counter, counter);
      counter := counter + 1;
   end loop;
end$$;