FROM node:22-alpine AS build

WORKDIR /app

ARG VITE_BACKEND_URL=""
ENV VITE_BACKEND_URL=$VITE_BACKEND_URL

# node:20-alpine's bundled npm (10.8.2) has a known bug ("Exit handler never
# called!") that can silently corrupt `npm ci` -- the step exits 0 but
# node_modules/.bin is left incomplete (e.g. missing vite). Node 22's bundled
# npm doesn't have it.
COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
