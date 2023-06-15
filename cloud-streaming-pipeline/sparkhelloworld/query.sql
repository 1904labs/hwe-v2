with words as (
    select explode(split(lcase(regexp_replace(value, '[^a-zA-Z ]', '')), ' ')) as word 
    from book
)
select word, count(*) as ct
from words
where len(word) > 0
group by word
order by ct desc