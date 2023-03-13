#!/bin/bash

ps --no-headers -C craafdd -opid | xargs -r kill -10

