from ipykernel.kernelapp import IPKernelApp

from marimo_zed.kernel import MarimoZedKernel

if __name__ == "__main__":
    IPKernelApp.launch_instance(kernel_class=MarimoZedKernel)
