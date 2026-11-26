// JINX Desktop Console — Native MyGo desktop interface for the JINX
// multi-source control plane.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"jinx-desktop/app"
	"jinx-desktop/assets"
	"jinx-desktop/client"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
)

func main() {
	apiURL := flag.String("api", "http://127.0.0.1:8090", "本地回环 JINX 控制面 / 网关 HTTP 根地址")
	mockMode := flag.Bool("mock", false, "使用内存隔离的合成沙盒数据源运行")
	scenario := flag.String("scenario", "full", "-mock 模式场景预设: full, degraded, conflict, unknown-delivery")
	darkMode := flag.Bool("dark", false, "启动时默认启用高对比度深色主题")
	flag.Parse()

	var c client.ConsoleClient
	if *mockMode {
		c = client.NewMockClient(*scenario)
	} else {
		hc, err := client.NewHTTPClient(*apiURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jinx-desktop: %v\n", err)
			os.Exit(2)
		}
		c = hc
	}

	store := app.NewStore(c, *darkMode)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mygo.App.SetName("JINX 控制台")
	mygo.App.WhenReady(func() {
		if err := mygo.App.Dock.SetIcon(assets.AppIconPNG); err != nil {
			fmt.Fprintf(os.Stderr, "jinx-desktop: 设置 Dock 图标失败: %v\n", err)
		}
		win := mygo.NewWindow(mygo.WindowOptions{
			Title:           "JINX 控制台",
			Width:           1380,
			Height:          900,
			MinWidth:        1080,
			MinHeight:       720,
			TitleBarStyle:   mygo.TitleBarHiddenInset,
			TitleBarHeight:  48,
			BackgroundColor: "light-dark(#F7F8FA, #15171A)",
			Content:         ui.View(store.View),
		})
		if err := win.SetIcon(assets.AppIconPNG); err != nil {
			fmt.Fprintf(os.Stderr, "jinx-desktop: 设置窗口图标失败: %v\n", err)
		}
		store.SetWindow(win)
		win.OnClose(func(_ *mygo.CloseEvent) {
			store.StopStreams()
			cancel()
		})
		store.Start(ctx)
	})

	if err := mygo.App.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "jinx-desktop: %v\n", err)
		os.Exit(1)
	}
}
