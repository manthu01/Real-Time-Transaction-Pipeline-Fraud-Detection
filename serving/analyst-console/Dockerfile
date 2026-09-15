# NEXT_PUBLIC_* vars are inlined into the client bundle at build time, not
# read at container start -- and this bundle runs in the *browser*, not
# inside the Docker network, so it must point at the alert-service's
# host-mapped port (localhost:3001), not its in-network hostname.
ARG NEXT_PUBLIC_API_BASE_URL=http://localhost:3001

FROM node:22-alpine AS build
ARG NEXT_PUBLIC_API_BASE_URL
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY next.config.js tsconfig.json next-env.d.ts ./
COPY src ./src
RUN npm run build

FROM node:22-alpine
ARG NEXT_PUBLIC_API_BASE_URL
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=build /app/.next ./.next
COPY next.config.js ./
EXPOSE 3000
CMD ["npx", "next", "start", "-p", "3000"]
