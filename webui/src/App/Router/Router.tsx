import { createElement } from "react";
import {
  createBrowserRouter,
  redirect,
  RouterProvider,
} from "react-router-dom";

import { MainLayout } from "shared/ui/layout/Main/MainLayout";
import { Routes } from "shared/router/config";
import { PageContainer } from "shared/ui/layout/PageContainer";
import { CenteredLayout } from "shared/ui/layout/Centered";
import { LoadingIndicator } from "shared/ui/LoadingIndicator";

import { Protected } from "./Protected";
import { NoAuth } from "./NoAuth";

const router = createBrowserRouter([
  {
    // Typical pages are not restricted when env auth is disabled
    element: createElement(Protected),
    children: [
      {
        Component: PageContainer,
        children: [
          {
            path: "/",
            loader: () => redirect(Routes.HOME),
            hydrateFallbackElement: createElement(LoadingIndicator),
          },
          {
            path: "*",
            loader: () => redirect(Routes.HOME),
            hydrateFallbackElement: createElement(LoadingIndicator),
          },
        ],
      },
      {
        Component: MainLayout,
        children: [
          {
            path: Routes.NODES,
            lazy: () => import("modules/nodes"),
            hydrateFallbackElement: createElement(LoadingIndicator),
          },
          {
            path: Routes.USERS,
            lazy: () => import("modules/users"),
            hydrateFallbackElement: createElement(LoadingIndicator),
          },
        ],
      },
    ],
  },
  {
    // Login page is restricted for both:
    // - authenticated users
    // - envs with disabled auth
    element: createElement(NoAuth),
    children: [
      {
        Component: CenteredLayout,
        children: [
          {
            path: Routes.LOGIN,
            lazy: () => import("modules/login"),
            hydrateFallbackElement: createElement(LoadingIndicator),
          },
        ],
      },
    ],
  },
]);

export function BrowserRouter() {
  return <RouterProvider router={router} />;
}
