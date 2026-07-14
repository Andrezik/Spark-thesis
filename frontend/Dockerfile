FROM node:24 AS build
WORKDIR /spark-gui
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /spark-gui/dist/kubernetes-api/browser /usr/share/nginx/html
COPY nginx/default.conf /etc/nginx/conf.d/default.conf
CMD ["nginx", "-g", "daemon off;"]