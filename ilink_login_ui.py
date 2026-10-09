"""User-operated native login UI. Never invoked by MCP; no stdout secrets."""
import json
import os
from pathlib import Path
import queue
import stat
import threading
import tkinter as tk
from tkinter import messagebox
from ilink_adapter import confirmed_binding, qr_request, qr_status_request, API_HOST
from ilink_transport import ILinkHTTPS

STATE_DIR = Path(__file__).parent / 'runtime' / 'wechat-private'


def save_new_binding(binding, directory=STATE_DIR):
    directory.mkdir(mode=0o700, parents=False, exist_ok=True)
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or info.st_mode & 0o077):
        raise ValueError('unsafe_private_directory')
    # Exclusive creation avoids reading or overwriting any previous credential.
    fd = os.open(directory / 'binding.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump({'bot_id': binding.bot_id, 'user_id': binding.user_id,
                       'token': binding.token, 'api_host': API_HOST}, stream)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        (directory / 'binding.json').unlink(missing_ok=True)
        raise


def main():
    # Existing OpenCV/Pillow only; no QR service, third-party page or disk image.
    import cv2
    from PIL import Image, ImageTk, ImageOps
    root = tk.Tk()
    root.title('dot 微信文字测试 — 本机授权')
    root.geometry('680x680')
    status = tk.StringVar(value='点击生成二维码才联网。仅绑定本人文字收发；请在手机阅读条款。')
    tk.Label(root, textvariable=status, wraplength=640).pack(pady=10)
    image_label = tk.Label(root)
    image_label.pack()
    tk.Label(root, text='手机验证码（仅需要时填写，不进入聊天/日志）').pack()
    verify = tk.Entry(root, show='*')
    verify.pack()
    events = queue.Queue()
    state = {'qrcode': None, 'confirmed': None, 'busy': False}
    transport = ILinkHTTPS()

    def job(operation):
        if state['busy']:
            return
        state['busy'] = True
        status.set('正在联系腾讯官方服务…')
        def worker():
            try:
                events.put(('result', operation()))
            except Exception as error:
                # Exception messages and server responses may contain secrets.
                category = str(error) if type(error).__name__ == 'CallbackError' else 'operation_failed'
                events.put(('error', category))
        threading.Thread(target=worker, daemon=True).start()

    def generate():
        if (STATE_DIR / 'binding.json').exists():
            status.set('本任务已存在绑定文件；未读取或覆盖。请报告已有绑定状态。')
            return
        job(lambda: ('qr', transport.request(*qr_request())))

    def poll():
        if not state['qrcode']:
            status.set('请先生成二维码。')
            return
        code = verify.get().strip() or None
        verify.delete(0, 'end')
        job(lambda: ('status', transport.request(*qr_status_request(state['qrcode'], code))))

    def persist():
        if state['confirmed'] is None:
            return
        if not messagebox.askyesno('由本人确认', '你是否已在手机亲自阅读并同意显示的条款，且权限未超出本人文字转接？\n确认后持久保存专用绑定凭据。'):
            return
        try:
            save_new_binding(state['confirmed'])
            state['confirmed'] = None
            state['qrcode'] = None
            image_label.configure(image='')
            status.set('专用绑定已保存。尚未开启消息轮询/转接；向父会话报告“绑定完成”即可。')
        except Exception:
            status.set('保存失败；没有输出凭据。请报告此非敏感状态。')

    def drain():
        try:
            kind, payload = events.get_nowait()
            state['busy'] = False
            if kind == 'error':
                status.set('安全停止：' + payload)
            else:
                action, data = payload
                if action == 'qr':
                    qrcode, content = data.get('qrcode'), data.get('qrcode_img_content')
                    if not isinstance(qrcode, str) or not isinstance(content, str) or max(len(qrcode), len(content)) > 8192:
                        raise ValueError()
                    state['qrcode'] = qrcode
                    matrix = cv2.QRCodeEncoder_create().encode(content)
                    image = ImageOps.expand(Image.fromarray(matrix), border=8, fill=255).resize((400, 400), Image.Resampling.NEAREST)
                    photo = ImageTk.PhotoImage(image)
                    image_label.configure(image=photo)
                    image_label.image = photo
                    status.set('请本人微信扫码并阅读手机条款；完成后点击检查状态。额外权限请先停止。')
                else:
                    value = data.get('status')
                    if value == 'confirmed':
                        state['confirmed'] = confirmed_binding(data)
                        status.set('腾讯返回已绑定。请点击“确认条款并保存”，凭据不会显示。')
                    elif value in ('scaned_but_redirect', 'binded_redirect'):
                        status.set('安全停止：需要新端点/已有绑定核实；未跟随、未读取旧凭据。')
                    elif value in ('wait', 'scaned', 'expired', 'need_verifycode', 'verify_code_blocked'):
                        status.set('腾讯授权状态：' + value + '。必要时本机填写验证码再检查。')
                    else:
                        status.set('安全停止：未知认证状态。')
        except queue.Empty:
            pass
        except Exception:
            status.set('安全停止：认证响应或二维码处理失败。')
        root.after(200, drain)

    tk.Button(root, text='生成专用二维码', command=generate).pack(pady=5)
    tk.Button(root, text='检查扫码状态 / 提交验证码', command=poll).pack(pady=5)
    tk.Button(root, text='确认条款并保存', command=persist).pack(pady=5)
    root.after(200, drain)
    root.mainloop()


if __name__ == '__main__':
    main()
