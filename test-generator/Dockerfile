FROM node:18

WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
RUN npm install
COPY src/* ./src/
RUN npx tsc

CMD ["out/index.js"]
