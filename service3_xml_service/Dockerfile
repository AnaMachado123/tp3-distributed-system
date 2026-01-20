FROM node:22-bullseye

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 9000
CMD ["node", "src/server.js"]
