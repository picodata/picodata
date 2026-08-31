## fix/backup

- `picodata restore` now removes vinyl runs written after the backup was taken.
  Previously they survived the restore and were never collected, and vinyl
  eventually handed out their identifiers again, so the first checkpoint after
  a restore failed because the file it was about to write already existed ([!3610]).
- Shredding no longer overwrites a data file under a name Picodata still
  considers live. A pass interrupted by a crash, or by the restart that
  `picodata restore` performs, used to leave a file full of random bytes where
  a valid snapshot or WAL was expected, which broke recovery.
