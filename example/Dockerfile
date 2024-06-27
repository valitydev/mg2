ARG ELIXIR_VERSION
ARG OTP_VERSION

FROM docker.io/library/elixir:${ELIXIR_VERSION}-otp-${OTP_VERSION}
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install woorl
RUN wget -q -O- "https://github.com/valitydev/woorl/releases/download/1.8/woorl-1.8.tar.gz" \
    | tar -xvz -C /usr/local/bin/

# Set env
ENV CHARSET=UTF-8
ENV LANG=C.UTF-8

RUN apt update && apt install -y inotify-tools && mix local.hex --force && mix local.rebar --force

# Set runtime
CMD ["/bin/bash"]
