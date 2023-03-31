do $$
declare 
   counter integer := 1;
begin
   while counter <= 3 loop
      insert into serializability_2 (a, b) VALUES (counter, counter);
      counter := counter + 1;
   end loop;
end$$;