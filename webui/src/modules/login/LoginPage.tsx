import {
  FormEventHandler,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { useNavigate } from "react-router";

import { useTranslation } from "shared/intl";
import logo from "assets/logo-dark.svg";
import { Button } from "shared/ui/Button/Button";
import { useLogin } from "shared/entity/session";
import { Routes } from "shared/router/config";
import { ApiError } from "shared/api/errors";
import { Checkbox } from "shared/ui/Checkbox";
import { useSessionStore } from "shared/session";

import { InputField } from "./InputField";

import styles from "./LoginPage.module.scss";
import layout from "shared/ui/layout/Centered/Centered.module.scss";

export const LoginPage = () => {
  const login = useLogin();
  const navigate = useNavigate();
  const [, session] = useSessionStore();

  const {
    errors,
    pages: {
      login: { form },
    },
  } = useTranslation().translation;

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const [usernameError, setUsernameError] = useState("");
  const [passwordError, setPasswordError] = useState("");

  const callLogin = useCallback<FormEventHandler<HTMLFormElement>>(
    (e) => {
      e.preventDefault();

      if (!username) {
        setUsernameError(form.field.requiredError);
        return;
      }

      if (!password) {
        setPasswordError(form.field.requiredError);
        return;
      }

      login
        .mutateAsync({
          username,
          password,
        })
        .then(() => {
          navigate(Routes.HOME);
        });
    },
    [form.field.requiredError, login, navigate, password, username]
  );

  useEffect(() => {
    if (login.error) {
      const errorKey =
        login.error.response?.data.error ?? ApiError.wrongCredentials;

      setPasswordError(errors[errorKey]);
    }
  }, [errors, login.error]);

  useEffect(() => {
    setUsernameError("");
  }, [username]);

  useEffect(() => {
    setPasswordError("");
  }, [password]);

  const isFormDisabled = useMemo(
    () => !username || !password,
    [password, username]
  );

  return (
    <>
      <main className={layout.main}>
        <div className={styles.loginContainer}>
          <img src={logo} alt="PICODATA" className={styles.logo} />

          <div className={styles.formContainer}>
            <h1 className={styles.title}>{form.title}</h1>

            <form
              className={styles.form}
              onSubmit={callLogin}
              aria-disabled={isFormDisabled}
            >
              <div className={styles.formFields}>
                <InputField
                  autoComplete="username"
                  name="username"
                  value={username}
                  setValue={setUsername}
                  error={usernameError}
                  {...form.field.username}
                />

                <InputField
                  autoComplete="current-password"
                  name="password"
                  toggleable
                  value={password}
                  setValue={setPassword}
                  error={passwordError}
                  {...form.field.password}
                />

                <div className={styles.formFieldPadded}>
                  <Checkbox
                    name="remember"
                    checked={session.remembers}
                    onChange={() => session.setRemembers((prev) => !prev)}
                  >
                    {form.field.remember.title}
                  </Checkbox>
                </div>
              </div>
              <div className={styles.formActions}>
                <Button>{form.loginButton}</Button>
              </div>
            </form>
          </div>
        </div>
      </main>
    </>
  );
};
