-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Expected values were recorded with the Lua implementation of to_char
-- (datetime:format), so they pin its output for every directive.

-- TEST: to-char-utc-names
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Tuesday Tue January Jan Jan PM'

-- TEST: to-char-utc-dates
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|26|2026|2026|26|01/13/26|2026-01-13|13-Jan-2026|01/13/26|Tue Jan 13 15:04:05 2026|Tue Jan 13 15:04:05  2026'

-- TEST: to-char-utc-numbers
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'13|13|15|03|15| 3|04|01|05|013|2|2|02|02|03'

-- TEST: to-char-utc-times
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'15:04|03:04:05 PM|15:04:05|15:04:05|1768316645|+0000|'

-- TEST: to-char-utc-fraction
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-utc-misc
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|26|15|01|%Y|q|3d|10Y|plain text'

-- TEST: to-char-offset-ms-names
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123+03:00'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Tuesday Tue January Jan Jan PM'

-- TEST: to-char-offset-ms-dates
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123+03:00'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|26|2026|2026|26|01/13/26|2026-01-13|13-Jan-2026|01/13/26|Tue Jan 13 15:04:05 2026|Tue Jan 13 15:04:05  2026'

-- TEST: to-char-offset-ms-numbers
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123+03:00'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'13|13|15|03|15| 3|04|01|05|013|2|2|02|02|03'

-- TEST: to-char-offset-ms-times
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123+03:00'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'15:04|03:04:05 PM|15:04:05|15:04:05|1768316645|+0300|'

-- TEST: to-char-offset-ms-fraction
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123+03:00'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'123|123|1|123|123000|123000000|123000000'

-- TEST: to-char-offset-ms-misc
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123+03:00'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|26|15|01|%Y|q|3d|10Y|plain text'

-- TEST: to-char-offset-us-names
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456-03:30'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Tuesday Tue January Jan Jan PM'

-- TEST: to-char-offset-us-dates
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456-03:30'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|26|2026|2026|26|01/13/26|2026-01-13|13-Jan-2026|01/13/26|Tue Jan 13 15:04:05 2026|Tue Jan 13 15:04:05  2026'

-- TEST: to-char-offset-us-numbers
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456-03:30'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'13|13|15|03|15| 3|04|01|05|013|2|2|02|02|03'

-- TEST: to-char-offset-us-times
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456-03:30'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'15:04|03:04:05 PM|15:04:05|15:04:05|1768316645|-0330|'

-- TEST: to-char-offset-us-fraction
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456-03:30'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'123456|123456|1|123|123456|123456000|123456000'

-- TEST: to-char-offset-us-misc
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456-03:30'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|26|15|01|%Y|q|3d|10Y|plain text'

-- TEST: to-char-offset-ns-names
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456789+05:45'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Tuesday Tue January Jan Jan PM'

-- TEST: to-char-offset-ns-dates
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456789+05:45'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|26|2026|2026|26|01/13/26|2026-01-13|13-Jan-2026|01/13/26|Tue Jan 13 15:04:05 2026|Tue Jan 13 15:04:05  2026'

-- TEST: to-char-offset-ns-numbers
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456789+05:45'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'13|13|15|03|15| 3|04|01|05|013|2|2|02|02|03'

-- TEST: to-char-offset-ns-times
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456789+05:45'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'15:04|03:04:05 PM|15:04:05|15:04:05|1768316645|+0545|'

-- TEST: to-char-offset-ns-fraction
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456789+05:45'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'123457|123457|1|123|123457|123457000|123457000'

-- TEST: to-char-offset-ns-misc
-- SQL:
SELECT to_char('2026-01-13T15:04:05.123456789+05:45'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|26|15|01|%Y|q|3d|10Y|plain text'

-- TEST: to-char-named-zone-names
-- SQL:
SELECT to_char('2026-07-01T23:30:00 Europe/Moscow'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Wednesday Wed July Jul Jul PM'

-- TEST: to-char-named-zone-dates
-- SQL:
SELECT to_char('2026-07-01T23:30:00 Europe/Moscow'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|26|2026|2026|26|07/01/26|2026-07-01| 1-Jul-2026|07/01/26|Wed Jul  1 23:30:00 2026|Wed Jul  1 23:30:00  2026'

-- TEST: to-char-named-zone-numbers
-- SQL:
SELECT to_char('2026-07-01T23:30:00 Europe/Moscow'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'01| 1|23|11|23|11|30|07|00|182|3|3|26|26|27'

-- TEST: to-char-named-zone-times
-- SQL:
SELECT to_char('2026-07-01T23:30:00 Europe/Moscow'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'23:30|11:30:00 PM|23:30:00|23:30:00|1782948600|+0300|'

-- TEST: to-char-named-zone-fraction
-- SQL:
SELECT to_char('2026-07-01T23:30:00 Europe/Moscow'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-named-zone-misc
-- SQL:
SELECT to_char('2026-07-01T23:30:00 Europe/Moscow'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|26|23|07|%Y|q|3d|10Y|plain text'

-- TEST: to-char-midnight-pm-names
-- SQL:
SELECT to_char('2026-07-04T00:00:00Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Saturday Sat July Jul Jul AM'

-- TEST: to-char-midnight-pm-dates
-- SQL:
SELECT to_char('2026-07-04T00:00:00Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|26|2026|2026|26|07/04/26|2026-07-04| 4-Jul-2026|07/04/26|Sat Jul  4 00:00:00 2026|Sat Jul  4 00:00:00  2026'

-- TEST: to-char-midnight-pm-numbers
-- SQL:
SELECT to_char('2026-07-04T00:00:00Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'04| 4|00|12| 0|12|00|07|00|185|6|6|26|26|27'

-- TEST: to-char-midnight-pm-times
-- SQL:
SELECT to_char('2026-07-04T00:00:00Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'00:00|12:00:00 AM|00:00:00|00:00:00|1783123200|+0000|'

-- TEST: to-char-midnight-pm-fraction
-- SQL:
SELECT to_char('2026-07-04T00:00:00Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-midnight-pm-misc
-- SQL:
SELECT to_char('2026-07-04T00:00:00Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|26|00|07|%Y|q|3d|10Y|plain text'

-- TEST: to-char-noon-names
-- SQL:
SELECT to_char('2026-07-04T12:00:00Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Saturday Sat July Jul Jul PM'

-- TEST: to-char-noon-dates
-- SQL:
SELECT to_char('2026-07-04T12:00:00Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|26|2026|2026|26|07/04/26|2026-07-04| 4-Jul-2026|07/04/26|Sat Jul  4 12:00:00 2026|Sat Jul  4 12:00:00  2026'

-- TEST: to-char-noon-numbers
-- SQL:
SELECT to_char('2026-07-04T12:00:00Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'04| 4|12|12|12|12|00|07|00|185|6|6|26|26|27'

-- TEST: to-char-noon-times
-- SQL:
SELECT to_char('2026-07-04T12:00:00Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'12:00|12:00:00 PM|12:00:00|12:00:00|1783166400|+0000|'

-- TEST: to-char-noon-fraction
-- SQL:
SELECT to_char('2026-07-04T12:00:00Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-noon-misc
-- SQL:
SELECT to_char('2026-07-04T12:00:00Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|26|12|07|%Y|q|3d|10Y|plain text'

-- TEST: to-char-leap-day-names
-- SQL:
SELECT to_char('2000-02-29T01:02:03Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Tuesday Tue February Feb Feb AM'

-- TEST: to-char-leap-day-dates
-- SQL:
SELECT to_char('2000-02-29T01:02:03Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|00|2000|2000|00|02/29/00|2000-02-29|29-Feb-2000|02/29/00|Tue Feb 29 01:02:03 2000|Tue Feb 29 01:02:03  2000'

-- TEST: to-char-leap-day-numbers
-- SQL:
SELECT to_char('2000-02-29T01:02:03Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'29|29|01|01| 1| 1|02|02|03|060|2|2|09|09|09'

-- TEST: to-char-leap-day-times
-- SQL:
SELECT to_char('2000-02-29T01:02:03Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'01:02|01:02:03 AM|01:02:03|01:02:03|951786123|+0000|'

-- TEST: to-char-leap-day-fraction
-- SQL:
SELECT to_char('2000-02-29T01:02:03Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-leap-day-misc
-- SQL:
SELECT to_char('2000-02-29T01:02:03Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|00|01|02|%Y|q|3d|10Y|plain text'

-- TEST: to-char-iso-week-53-names
-- SQL:
SELECT to_char('2020-12-31T08:00:00Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Thursday Thu December Dec Dec AM'

-- TEST: to-char-iso-week-53-dates
-- SQL:
SELECT to_char('2020-12-31T08:00:00Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|20|2020|2020|20|12/31/20|2020-12-31|31-Dec-2020|12/31/20|Thu Dec 31 08:00:00 2020|Thu Dec 31 08:00:00  2020'

-- TEST: to-char-iso-week-53-numbers
-- SQL:
SELECT to_char('2020-12-31T08:00:00Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'31|31|08|08| 8| 8|00|12|00|366|4|4|52|52|53'

-- TEST: to-char-iso-week-53-times
-- SQL:
SELECT to_char('2020-12-31T08:00:00Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'08:00|08:00:00 AM|08:00:00|08:00:00|1609401600|+0000|'

-- TEST: to-char-iso-week-53-fraction
-- SQL:
SELECT to_char('2020-12-31T08:00:00Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-iso-week-53-misc
-- SQL:
SELECT to_char('2020-12-31T08:00:00Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|20|08|12|%Y|q|3d|10Y|plain text'

-- TEST: to-char-iso-week-prev-year-names
-- SQL:
SELECT to_char('2021-01-03T08:00:00Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Sunday Sun January Jan Jan AM'

-- TEST: to-char-iso-week-prev-year-dates
-- SQL:
SELECT to_char('2021-01-03T08:00:00Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|21|2021|2020|20|01/03/21|2021-01-03| 3-Jan-2021|01/03/21|Sun Jan  3 08:00:00 2021|Sun Jan  3 08:00:00  2021'

-- TEST: to-char-iso-week-prev-year-numbers
-- SQL:
SELECT to_char('2021-01-03T08:00:00Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'03| 3|08|08| 8| 8|00|01|00|003|7|0|01|00|53'

-- TEST: to-char-iso-week-prev-year-times
-- SQL:
SELECT to_char('2021-01-03T08:00:00Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'08:00|08:00:00 AM|08:00:00|08:00:00|1609660800|+0000|'

-- TEST: to-char-iso-week-prev-year-fraction
-- SQL:
SELECT to_char('2021-01-03T08:00:00Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-iso-week-prev-year-misc
-- SQL:
SELECT to_char('2021-01-03T08:00:00Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|21|08|01|%Y|q|3d|10Y|plain text'

-- TEST: to-char-iso-week-next-year-names
-- SQL:
SELECT to_char('2024-12-30T08:00:00Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Monday Mon December Dec Dec AM'

-- TEST: to-char-iso-week-next-year-dates
-- SQL:
SELECT to_char('2024-12-30T08:00:00Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|24|2024|2025|25|12/30/24|2024-12-30|30-Dec-2024|12/30/24|Mon Dec 30 08:00:00 2024|Mon Dec 30 08:00:00  2024'

-- TEST: to-char-iso-week-next-year-numbers
-- SQL:
SELECT to_char('2024-12-30T08:00:00Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'30|30|08|08| 8| 8|00|12|00|365|1|1|52|53|01'

-- TEST: to-char-iso-week-next-year-times
-- SQL:
SELECT to_char('2024-12-30T08:00:00Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'08:00|08:00:00 AM|08:00:00|08:00:00|1735545600|+0000|'

-- TEST: to-char-iso-week-next-year-fraction
-- SQL:
SELECT to_char('2024-12-30T08:00:00Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-iso-week-next-year-misc
-- SQL:
SELECT to_char('2024-12-30T08:00:00Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|24|08|12|%Y|q|3d|10Y|plain text'

-- TEST: to-char-year-start-sunday-names
-- SQL:
SELECT to_char('2023-01-01T00:00:00Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Sunday Sun January Jan Jan AM'

-- TEST: to-char-year-start-sunday-dates
-- SQL:
SELECT to_char('2023-01-01T00:00:00Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'20|23|2023|2022|22|01/01/23|2023-01-01| 1-Jan-2023|01/01/23|Sun Jan  1 00:00:00 2023|Sun Jan  1 00:00:00  2023'

-- TEST: to-char-year-start-sunday-numbers
-- SQL:
SELECT to_char('2023-01-01T00:00:00Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'01| 1|00|12| 0|12|00|01|00|001|7|0|01|00|52'

-- TEST: to-char-year-start-sunday-times
-- SQL:
SELECT to_char('2023-01-01T00:00:00Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'00:00|12:00:00 AM|00:00:00|00:00:00|1672531200|+0000|'

-- TEST: to-char-year-start-sunday-fraction
-- SQL:
SELECT to_char('2023-01-01T00:00:00Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-year-start-sunday-misc
-- SQL:
SELECT to_char('2023-01-01T00:00:00Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|23|00|01|%Y|q|3d|10Y|plain text'

-- TEST: to-char-small-year-names
-- SQL:
SELECT to_char('0099-03-04T05:06:07Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Wednesday Wed March Mar Mar AM'

-- TEST: to-char-small-year-dates
-- SQL:
SELECT to_char('0099-03-04T05:06:07Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'00|99|0099|0099|99|03/04/99|0099-03-04| 4-Mar-0099|03/04/99|Wed Mar  4 05:06:07 0099|Wed Mar  4 05:06:07  0099'

-- TEST: to-char-small-year-numbers
-- SQL:
SELECT to_char('0099-03-04T05:06:07Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'04| 4|05|05| 5| 5|06|03|07|063|3|3|09|09|10'

-- TEST: to-char-small-year-times
-- SQL:
SELECT to_char('0099-03-04T05:06:07Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'05:06|05:06:07 AM|05:06:07|05:06:07|-59037620033|+0000|'

-- TEST: to-char-small-year-fraction
-- SQL:
SELECT to_char('0099-03-04T05:06:07Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-small-year-misc
-- SQL:
SELECT to_char('0099-03-04T05:06:07Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|99|05|03|%Y|q|3d|10Y|plain text'

-- TEST: to-char-year-1-names
-- SQL:
SELECT to_char('0001-01-01T00:00:00Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Monday Mon January Jan Jan AM'

-- TEST: to-char-year-1-dates
-- SQL:
SELECT to_char('0001-01-01T00:00:00Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'00|01|0001|0001|01|01/01/01|0001-01-01| 1-Jan-0001|01/01/01|Mon Jan  1 00:00:00 0001|Mon Jan  1 00:00:00  0001'

-- TEST: to-char-year-1-numbers
-- SQL:
SELECT to_char('0001-01-01T00:00:00Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'01| 1|00|12| 0|12|00|01|00|001|1|1|00|01|01'

-- TEST: to-char-year-1-times
-- SQL:
SELECT to_char('0001-01-01T00:00:00Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'00:00|12:00:00 AM|00:00:00|00:00:00|-62135596800|+0000|'

-- TEST: to-char-year-1-fraction
-- SQL:
SELECT to_char('0001-01-01T00:00:00Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-year-1-misc
-- SQL:
SELECT to_char('0001-01-01T00:00:00Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|01|00|01|%Y|q|3d|10Y|plain text'

-- TEST: to-char-year-9999-names
-- SQL:
SELECT to_char('9999-12-31T23:59:59Z'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Friday Fri December Dec Dec PM'

-- TEST: to-char-year-9999-dates
-- SQL:
SELECT to_char('9999-12-31T23:59:59Z'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'99|99|9999|9999|99|12/31/99|9999-12-31|31-Dec-9999|12/31/99|Fri Dec 31 23:59:59 9999|Fri Dec 31 23:59:59  9999'

-- TEST: to-char-year-9999-numbers
-- SQL:
SELECT to_char('9999-12-31T23:59:59Z'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'31|31|23|11|23|11|59|12|59|365|5|5|52|52|52'

-- TEST: to-char-year-9999-times
-- SQL:
SELECT to_char('9999-12-31T23:59:59Z'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'23:59|11:59:59 PM|23:59:59|23:59:59|253402300799|+0000|'

-- TEST: to-char-year-9999-fraction
-- SQL:
SELECT to_char('9999-12-31T23:59:59Z'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-year-9999-misc
-- SQL:
SELECT to_char('9999-12-31T23:59:59Z'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|99|23|12|%Y|q|3d|10Y|plain text'

-- TEST: to-char-before-epoch-names
-- SQL:
SELECT to_char('1969-12-31T23:59:59-01:00'::datetime, '%A %a %B %b %h %p');
-- EXPECTED:
'Wednesday Wed December Dec Dec PM'

-- TEST: to-char-before-epoch-dates
-- SQL:
SELECT to_char('1969-12-31T23:59:59-01:00'::datetime, '%C|%y|%Y|%G|%g|%D|%F|%v|%x|%c|%+');
-- EXPECTED:
'19|69|1969|1970|70|12/31/69|1969-12-31|31-Dec-1969|12/31/69|Wed Dec 31 23:59:59 1969|Wed Dec 31 23:59:59  1969'

-- TEST: to-char-before-epoch-numbers
-- SQL:
SELECT to_char('1969-12-31T23:59:59-01:00'::datetime, '%d|%e|%H|%I|%k|%l|%M|%m|%S|%j|%u|%w|%U|%W|%V');
-- EXPECTED:
'31|31|23|11|23|11|59|12|59|365|3|3|52|52|01'

-- TEST: to-char-before-epoch-times
-- SQL:
SELECT to_char('1969-12-31T23:59:59-01:00'::datetime, '%R|%r|%T|%X|%s|%z|%Z');
-- EXPECTED:
'23:59|11:59:59 PM|23:59:59|23:59:59|-1|-0100|'

-- TEST: to-char-before-epoch-fraction
-- SQL:
SELECT to_char('1969-12-31T23:59:59-01:00'::datetime, '%f|%0f|%1f|%3f|%6f|%9f|%12f');
-- EXPECTED:
'000|000|0|000|000000|000000000|000000000'

-- TEST: to-char-before-epoch-misc
-- SQL:
SELECT to_char('1969-12-31T23:59:59-01:00'::datetime, '%%|%Ey|%OH|%EOm|%E%Y|%q|%3d|%10Y|plain text');
-- EXPECTED:
'%|69|23|12|%Y|q|3d|10Y|plain text'

-- TEST: to-char-newline-and-tab
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, 'a%nb%tc') LIKE 'a_b_c';
-- EXPECTED:
true

-- TEST: to-char-null-datetime
-- SQL:
SELECT to_char(NULL, '%Y');
-- EXPECTED:
NULL

-- TEST: to-char-null-format
-- SQL:
SELECT to_char('2026-01-13T15:04:05Z'::datetime, NULL);
-- EXPECTED:
NULL

-- TEST: to-char-table
-- SQL:
CREATE TABLE to_char_t (id INT PRIMARY KEY, ts DATETIME);
INSERT INTO to_char_t VALUES
    (1, '2026-01-13T15:04:05Z'::datetime),
    (2, '2026-01-13T15:04:59+03:00'::datetime),
    (3, '2026-01-13T15:05:00Z'::datetime),
    (4, NULL);

-- TEST: to-char-table-group-by
-- SQL:
SELECT to_char(ts, '%Y-%m-%d %H:%M') AS m, count(*) FROM to_char_t GROUP BY to_char(ts, '%Y-%m-%d %H:%M') ORDER BY m;
-- EXPECTED:
NULL, 1,
'2026-01-13 15:04', 2,
'2026-01-13 15:05', 1

-- TEST: to-char-table-cleanup
-- SQL:
DROP TABLE to_char_t;
