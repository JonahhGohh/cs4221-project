do $$
declare
   counter integer := 1;
begin
   while counter <= 1000000 loop
      insert into indexing (id, age) VALUES (counter, mod(counter, 100)::int);
      counter := counter + 1;
   end loop;
end$$;
