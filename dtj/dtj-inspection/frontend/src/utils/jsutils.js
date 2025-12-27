import {Notify} from 'quasar';


// Константы для уведомлений
const NOTIFICATION_DEFAULTS = {
  POSITION: 'bottom-right',
  TIMEOUTS: {
    SUCCESS: 1000,
    ERROR: 5000,
    INFO: 5000
  },
  TYPES: {
    SUCCESS: 'positive',
    ERROR: 'negative',
    INFO: 'info'
  }
};


// Функции уведомлений
const notify = (message, type, timeout) => {
  if (!message) return;

  Notify.create({
    type,
    position: NOTIFICATION_DEFAULTS.POSITION,
    timeout,
    message,
  });
};

const notifySuccess = (msg, timeout = NOTIFICATION_DEFAULTS.TIMEOUTS.SUCCESS) =>
  notify(msg, NOTIFICATION_DEFAULTS.TYPES.SUCCESS, timeout);

const notifyError = (msg) =>
  notify(msg, NOTIFICATION_DEFAULTS.TYPES.ERROR, NOTIFICATION_DEFAULTS.TIMEOUTS.ERROR);

const notifyInfo = (msg) =>
  notify(msg, NOTIFICATION_DEFAULTS.TYPES.INFO, NOTIFICATION_DEFAULTS.TIMEOUTS.INFO);

export {
  notifySuccess,
  notifyError,
  notifyInfo,
};
