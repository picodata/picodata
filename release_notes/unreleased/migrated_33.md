## fix

- Revoking privileges from `admin` user caused a panic. Now, revoking priviliges
  from `admin` user is forbidden, for same reasons as for the `pico_service` user.
