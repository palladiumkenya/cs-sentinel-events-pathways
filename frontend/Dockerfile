FROM node:18-alpine AS build

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# Stage 2: Serve the app with 'serve'
FROM node:18-alpine

RUN npm install -g serve
WORKDIR /app
COPY --from=build /app/build ./build
CMD ["serve", "-s", "build", "-l", "3000"]
EXPOSE 3000